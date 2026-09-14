"""Pull all vendor statement files for a reporting month.

Invoked hourly on day 1 by EventBridge Scheduler. Exits immediately if the
month has already been processed, so the extra invocations are free no-ops.

Optional event overrides:
    {"period": "Aug26"}     - explicit vendor folder name
    {"period": "2026-08"}   - same month, ISO form
    {"force": true}         - ignore the run marker (backfill / re-run)
"""

import boto3
import json
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

transfer = boto3.client("transfer")
s3 = boto3.client("s3")
sns = boto3.client("sns")

CONNECTOR_ID = os.environ["CONNECTOR_ID"]
BASE_DIR = os.environ["REMOTE_DIRECTORY_BASE_PATH"].rstrip("/")
BUCKET = os.environ["BUCKET_NAME"]
ALERT_TOPIC_ARN = os.environ["ALERT_TOPIC_ARN"]
MAX_LISTING_ITEMS = int(os.environ.get("MAX_LISTING_ITEMS", "10000"))
FINAL_ATTEMPT_HOUR = int(os.environ.get("FINAL_ATTEMPT_HOUR", "23"))

# Must match the schedule's ScheduleExpressionTimezone. If the schedule fires
# at local midnight on the 1st but this resolves the period in UTC, the two
# disagree about which month it is and the wrong vendor folder gets targeted.
TZ = ZoneInfo(os.environ.get("TIMEZONE", "UTC"))

# Hard API limit - StartFileTransfer accepts at most 10 RetrieveFilePaths.
BATCH_SIZE = 10
POLL_SECONDS = 5
MAX_POLLS = 60
THROTTLE_BASE_DELAY = 5
THROTTLE_MAX_RETRIES = 6

NOT_FOUND = ("404", "NoSuchKey", "NotFound")


def alert(subject, message):
    # SNS delivers this to SMS subscribers, so keep it short. Long messages
    # are split across multiple billed segments.
    try:
        sns.publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject[:100],
                    Message=message[:900])
    except Exception as e:  # never let alerting mask the original failure
        print(f"WARN could not publish alert: {e}")


def resolve_period(event):
    """Return (period_key, vendor_folder) e.g. ("2026-08", "Aug26").

    The vendor publishes August's statements into Aug26 on 1 September, so
    the default is always the month that just closed.
    """
    override = (event or {}).get("period")
    if override:
        fmt = "%Y-%m" if "-" in override else "%b%y"
        dt = datetime.strptime(override, fmt)
    else:
        now = datetime.now(TZ)
        dt = now.replace(day=1) - timedelta(days=1)
    return dt.strftime("%Y-%m"), dt.strftime("%b%y")


def marker_key(period):
    return f"state/{period}.run.json"


def marker_exists(period):
    try:
        s3.head_object(Bucket=BUCKET, Key=marker_key(period))
        return True
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in NOT_FOUND:
            return False
        raise


def write_marker(period, payload):
    s3.put_object(Bucket=BUCKET, Key=marker_key(period),
                  Body=json.dumps(payload, indent=2).encode("utf-8"),
                  ContentType="application/json")


def wait_for_object(key):
    for _ in range(MAX_POLLS):
        try:
            return s3.head_object(Bucket=BUCKET, Key=key)
        except s3.exceptions.ClientError:
            time.sleep(POLL_SECONDS)
    return None


def listing_key(resp):
    """Resolve the S3 key the connector wrote its listing to.

    OutputFileName comes back bare, absolute, or bucket-qualified depending
    on the call. Documented naming is connector-ID-listing-ID.json, which is
    the fallback when the field is absent.
    """
    out = (resp.get("OutputFileName") or "").lstrip("/")
    if out.startswith(BUCKET + "/"):
        out = out[len(BUCKET) + 1:]
    if out:
        return out if out.startswith("listings/") else f"listings/{out.rsplit('/', 1)[-1]}"
    return f"listings/{CONNECTOR_ID}-{resp.get('ListingId', '')}.json"


def list_remote_files(remote_dir):
    """Return filenames in the vendor folder, or None if it is not there yet.

    None means "not ready" - on 1 September at 00:00 the Aug26 folder may not
    exist at all. That is expected, not a failure.
    """
    try:
        resp = transfer.start_directory_listing(
            ConnectorId=CONNECTOR_ID,
            RemoteDirectoryPath=remote_dir,
            OutputDirectoryPath=f"/{BUCKET}/listings",
            MaxItems=MAX_LISTING_ITEMS)
    except transfer.exceptions.ResourceNotFoundException:
        print(f"Remote directory {remote_dir} not found yet")
        return None

    key = listing_key(resp)
    if wait_for_object(key) is None:
        print(f"Listing {key} never appeared - treating as not ready")
        return None

    body = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    if body.get("truncated"):
        raise RuntimeError(
            f"Listing of {remote_dir} was truncated at {MAX_LISTING_ITEMS} items. "
            "Raise MaxListingItems.")

    names = []
    for f in body.get("files", []):
        name = (f.get("filePath") or "").rsplit("/", 1)[-1]
        if name:
            names.append(name)
    return sorted(names)


def start_batch(paths, dest_prefix):
    """One StartFileTransfer call, retried on throttling.

    The connector queue caps at 1000 pending transfers and is shared with
    account-sync, so a large month can be rejected with ThrottlingException
    ("Exceeded maximum pending requests"). Backing off lets the queue drain.
    """
    delay = THROTTLE_BASE_DELAY
    for attempt in range(THROTTLE_MAX_RETRIES + 1):
        try:
            return transfer.start_file_transfer(
                ConnectorId=CONNECTOR_ID,
                RetrieveFilePaths=paths,
                LocalDirectoryPath=f"/{BUCKET}/{dest_prefix}")
        except transfer.exceptions.ThrottlingException:
            if attempt == THROTTLE_MAX_RETRIES:
                raise
            print(f"Throttled, backing off {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 60)


def transfer_batches(remote_dir, names, dest_prefix):
    """StartFileTransfer accepts 10 paths per call, so ~60 files is 6 calls."""
    transfer_ids = []
    for i in range(0, len(names), BATCH_SIZE):
        batch = names[i:i + BATCH_SIZE]
        resp = start_batch([f"{remote_dir}/{n}" for n in batch], dest_prefix)
        transfer_ids.append(resp["TransferId"])
        print(f"Batch {len(transfer_ids)}: {len(batch)} files, "
              f"TransferId={resp['TransferId']}")
    return transfer_ids


def is_final_attempt():
    return datetime.now(TZ).hour >= FINAL_ATTEMPT_HOUR


def lambda_handler(event, context):
    event = event or {}
    period, folder = resolve_period(event)
    remote_dir = f"{BASE_DIR}/{folder}"
    dest_prefix = f"incoming/{period}"

    try:
        if not event.get("force") and marker_exists(period):
            print(f"{period} already processed - exiting")
            return {"status": "already_processed", "period": period}

        names = list_remote_files(remote_dir)

        if not names:
            if is_final_attempt():
                alert(f"Statement sync: no files for {period}",
                      f"Nothing found in {remote_dir} by end of day. "
                      f"Vendor drop may be late.")
            return {"status": "not_ready", "period": period,
                    "remoteDirectory": remote_dir}

        transfer_ids = transfer_batches(remote_dir, names, dest_prefix)

        write_marker(period, {
            "period": period,
            "remoteDirectory": remote_dir,
            "destinationPrefix": dest_prefix,
            "fileCount": len(names),
            "files": names,
            "transferIds": transfer_ids,
            "startedAt": datetime.now(TZ).isoformat(),
        })

        return {"status": "transfer_started", "period": period,
                "fileCount": len(names), "transferIds": transfer_ids,
                "destinationPrefix": dest_prefix}

    except Exception as e:
        alert(f"Statement sync FAILED for {period}", f"{type(e).__name__}: {e}")
        raise
