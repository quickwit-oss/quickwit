import json
import os
import random
import time
import gzip
import random

import boto3

s3 = boto3.client("s3")

with gzip.open("wordlist.json.gz", "r") as f:
    words: list[str] = json.loads(f.read())


def lambda_handler(event, context):
    data = []
    base_time = time.time()
    for i in range(100000):
        item = {
            "ts": int(base_time * 1000) + i,
            "id": i,
            "name": f"Item {i}",
            "price": round(random.uniform(1, 100), 2),
            "quantity": random.randint(1, 10),
            "description": " ".join(random.choices(words, k=8)),
        }
        data.append(item)

    json_str = "\n".join(json.dumps(d) for d in data)
    json_gz = gzip.compress(json_str.encode(), compresslevel=6)

    s3.put_object(
        Bucket=os.environ["BUCKET_NAME"],
        Key=f"{os.environ['PREFIX']}/{int(base_time)}.gz",
        Body=json_gz,
    )
