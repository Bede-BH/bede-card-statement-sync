"""Decrypt one vendor statement file.

Triggered by S3 ObjectCreated on incoming/*.pgp. One invocation per file,
so ~60 files decrypt in parallel rather than in a loop.

    incoming/2026-08/BEDE_STMT_0001.csv.pgp
        -> decrypted/2026-08/BEDE_STMT_0001.csv
"""

import boto3
import mimetypes
import os
import urllib.parse
import pgpy

s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")
sns = boto3.client("sns")

KEY_SECRET_NAME = os.environ["PGP_KEY_SECRET_NAME"]
PASSPHRASE_SECRET_NAME = os.environ.get("PGP_PASSPHRASE_SECRET_NAME") or ""
ALERT_TOPIC_ARN = os.environ["ALERT_TOPIC_ARN"]

_key = None
_passphrase = None


def alert(subject, message):
    try:
        sns.publish(TopicArn=ALERT_TOPIC_ARN, Subject=subject[:100],
                    Message=message[:900])
    except Exception as e:
        print(f"WARN could not publish alert: {e}")


def load_key():
    # Cached across invocations in the same container - one Secrets Manager
    # call per cold start rather than one per file. Matters here: 60 files
    # arriving at once would otherwise be 60 concurrent GetSecretValue calls.
    global _key, _passphrase
    if _key is None:
        blob = secrets.get_secret_value(SecretId=KEY_SECRET_NAME)["SecretString"]
        _key, _ = pgpy.PGPKey.from_blob(blob)
        if _key.is_protected:
            if not PASSPHRASE_SECRET_NAME:
                raise RuntimeError(
                    "PGP key is passphrase-protected but PGP_PASSPHRASE_SECRET_NAME is not set")
            _passphrase = secrets.get_secret_value(
                SecretId=PASSPHRASE_SECRET_NAME)["SecretString"]
    return _key, _passphrase


def decrypt(key, passphrase, msg):
    if not key.is_protected:
        return key.decrypt(msg).message
    # A passphrase pasted into the console often picks up a trailing newline;
    # try it verbatim first, then stripped.
    last = None
    for candidate in (passphrase, passphrase.strip()):
        try:
            with key.unlock(candidate):
                return key.decrypt(msg).message
        except Exception as e:
            last = e
    raise RuntimeError(f"Could not unlock the PGP key with the stored passphrase: {last}")


def dest_key(src_key):
    parts = src_key.split("/")
    period = parts[1] if len(parts) > 2 else "unknown"
    name = parts[-1]
    low = name.lower()
    if low.endswith(".pgp") or low.endswith(".gpg"):
        name = name[:-4]
    return f"decrypted/{period}/{name}"


def process(bucket, src_key):
    ciphertext = s3.get_object(Bucket=bucket, Key=src_key)["Body"].read()

    key, passphrase = load_key()
    plaintext = decrypt(key, passphrase, pgpy.PGPMessage.from_blob(ciphertext))
    if not isinstance(plaintext, (bytes, bytearray)):
        plaintext = plaintext.encode("utf-8")

    out_key = dest_key(src_key)
    # Statements may not be CSV - infer rather than hardcode.
    content_type = mimetypes.guess_type(out_key)[0] or "application/octet-stream"

    put = s3.put_object(Bucket=bucket, Key=out_key, Body=plaintext,
                        ContentType=content_type)

    print(f"{src_key} -> {out_key} ({len(plaintext)} bytes)")
    return {
        "sourceS3Key": src_key,
        "decryptedS3Key": out_key,
        "decryptedVersionId": put.get("VersionId"),
        "decryptedSizeBytes": len(plaintext),
    }


def lambda_handler(event, context):
    results = []
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        # S3 URL-encodes the key in the event. Vendor filenames with spaces
        # arrive as "BEDE+STMT.csv.pgp" and GetObject would 404 on that.
        src_key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        try:
            results.append(process(bucket, src_key))
        except Exception as e:
            alert("Statement decrypt FAILED",
                  f"{src_key}\n{type(e).__name__}: {e}")
            raise
    return {"decrypted": results}
