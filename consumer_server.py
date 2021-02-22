import asyncio
from dataclasses import dataclass, field
import json

from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:3456"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        messages = c.consume(5, timeout=1.0)
        print(f"consumed {len(messages)} messages")
        for message in messages:
            print(f"consume message {message.key()}: {message.value()}")
        await asyncio.sleep(0.01)

def consumer():
    asyncio.run(consume("police.service.calls"))

if __name__ == '__main__':
    consumer()