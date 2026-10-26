#!/bin/sh
# One-shot, runs in the curl image: create the lake bucket with a SigV4-signed PUT.
# Credentials come from the environment and are never echoed (no `set -x`).
set -eu

BUCKET="${S3_BUCKET:-lakehouse}"
S3_REGION="${S3_REGION:-us-east-1}"
body=$(mktemp)
# Empty-body SHA-256, so signing does not depend on curl's payload-hash defaults.
EMPTY_SHA="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

attempt=0
while [ "$attempt" -lt 20 ]; do
    code=$(curl -s -o "$body" -w '%{http_code}' \
        --aws-sigv4 "aws:amz:${S3_REGION}:s3" --user "${S3_ACCESS_KEY}:${S3_SECRET_KEY}" \
        -H "x-amz-content-sha256: ${EMPTY_SHA}" \
        -X PUT "${S3_ENDPOINT}/${BUCKET}" || true)
    if [ "$code" = "200" ]; then
        echo "Bucket ${BUCKET} is ready"
        exit 0
    fi
    if [ "$code" = "409" ] && grep -q "BucketAlreadyOwnedByYou" "$body"; then
        echo "Bucket ${BUCKET} already exists"
        exit 0
    fi
    attempt=$((attempt + 1))
    echo "Bucket create attempt ${attempt} got HTTP ${code:-000}; retrying" >&2
    sleep 3
done

echo "Could not create bucket ${BUCKET}. Last response:" >&2
cat "$body" >&2
exit 1
