#!/bin/bash

set -u

# to have auth, add this:
# --cert path_to/cert.crt --key path_to/key.key

# status code list : https://grpc.github.io/grpc/core/md_doc_statuscodes.html

PAYLOAD="AAAAAAA="

STATUS=$(echo $PAYLOAD | base64 -d | curl "https://$1:443/cloudprem.CloudPremService/Ping" -H "Content-Type: application/x-protobuf"  --http2-prior-knowledge --data-binary @- -qv -o /dev/null 2>&1| sed -nE 's/.*(grpc-[a-z]*: [0-9a-zA-W%])/\1/p')

if [[ "$STATUS" = *"grpc-status: 0"* ]]; then
  echo "request accepted as authenticated"
elif [[ "$STATUS" = *"grpc-status: 16"* ]]; then
  echo "request rejected as unauthenticated"
else
  echo "request rejected for unknwon reason: $STATUS"
fi
