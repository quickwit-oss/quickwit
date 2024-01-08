import json
import os
import random
import time

import boto3

s3 = boto3.client("s3")


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
            "description": f"This is a description for Item {i}.",
        }
        data.append(item)
    json_data = "\n".join([json.dumps(d) for d in data])
    key = int(base_time)
    s3.put_object(
        Bucket=os.environ["BUCKET_NAME"],
        Key=f"{os.environ['PREFIX']}/{key}",
        Body=json_data,
    )
