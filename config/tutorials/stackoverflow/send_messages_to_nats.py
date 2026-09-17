import asyncio

import nats


async def main():
    client = await nats.connect("nats://localhost:4222")
    jetstream = client.jetstream()

    with open("stackoverflow.posts.transformed-10000.json", encoding="utf8") as file:
        for i, line in enumerate(file):
            await jetstream.publish("stackoverflow.posts", line.strip().encode("utf-8"))
            if i % 1000 == 0:
                print(f"{i}/10000 messages sent.")

    await client.close()


asyncio.run(main())
