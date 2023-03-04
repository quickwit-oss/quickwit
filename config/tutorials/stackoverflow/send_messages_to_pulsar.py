import json
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer('stackoverflow')

with open('stackoverflow.posts.transformed-10000.json', encoding='utf8') as file:
   for i, line in enumerate(file):
       producer.send(line.encode('utf-8'))
       if i % 100 == 0:
           print(f"{i}/10000 messages sent.", i)

client.close()
