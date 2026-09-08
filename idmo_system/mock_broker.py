"""
Minimal in-process mocks for aiokafka and nats.py, for demos/tests.

- MockKafkaBroker + MockAIOKafkaProducer/Consumer mimic the aiokafka API
  (start/stop, send_and_wait, async-iteration over a consumer).
- MockNATSBroker + MockNATSClient mimic the nats.py API
  (connect, publish, subscribe with either callback or pull-style next_msg).

Both are pure pub/sub over asyncio.Queue -- no persistence, no consumer
groups/load-balancing, no ack semantics. That's intentional: this is only
meant to let you demo the data flow within a single process.
"""

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Optional, Awaitable


# ============================================================
# Kafka mock
# ============================================================

@dataclass
class KafkaMessage:
    topic: str
    value: Any
    key: Optional[bytes] = None


class MockKafkaBroker:
    """Shared state for one 'cluster'. Create one instance and pass it
    to every producer/consumer that should see each other."""

    def __init__(self):
        self._topics: dict[str, list[asyncio.Queue]] = {}

    def register_consumer(self, topic: str, queue: asyncio.Queue):
        self._topics.setdefault(topic, []).append(queue)

    def unregister_consumer(self, topic: str, queue: asyncio.Queue):
        subs = self._topics.get(topic, [])
        if queue in subs:
            subs.remove(queue)

    async def publish(self, topic: str, value: Any, key: Optional[bytes] = None):
        msg = KafkaMessage(topic=topic, value=value, key=key)
        for q in self._topics.get(topic, []):
            await q.put(msg)


class MockAIOKafkaProducer:
    """Drop-in replacement for aiokafka.AIOKafkaProducer (subset used
    for demos: start/stop/send_and_wait)."""

    def __init__(self, broker: MockKafkaBroker, **kwargs):
        self._broker = broker

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send_and_wait(self, topic: str, value: Any, key: Optional[bytes] = None):
        await self._broker.publish(topic, value, key=key)

    # aiokafka's fire-and-forget send() returns a future; here we just
    # await it directly since everything is in-process anyway.
    async def send(self, topic: str, value: Any, key: Optional[bytes] = None):
        await self.send_and_wait(topic, value, key=key)


class MockAIOKafkaConsumer:
    """Drop-in replacement for aiokafka.AIOKafkaConsumer (subset used
    for demos: start/stop/async-for). group_id is accepted but ignored --
    every consumer instance gets every message (fan-out, not
    load-balanced consumer-group semantics)."""

    def __init__(self, *topics: str, broker: MockKafkaBroker, group_id: Optional[str] = None, **kwargs):
        self._broker = broker
        self._topics = topics
        self._queue: asyncio.Queue = asyncio.Queue()

    async def start(self):
        for t in self._topics:
            self._broker.register_consumer(t, self._queue)

    async def stop(self):
        for t in self._topics:
            self._broker.unregister_consumer(t, self._queue)

    async def getone(self) -> KafkaMessage:
        return await self._queue.get()

    def __aiter__(self):
        return self

    async def __anext__(self) -> KafkaMessage:
        return await self._queue.get()


# ============================================================
# NATS mock
# ============================================================

@dataclass
class NatsMessage:
    subject: str
    data: bytes


class MockNATSSubscription:
    def __init__(self, broker: "MockNATSBroker", subject: str,
                 cb: Optional[Callable[[NatsMessage], Awaitable[None]]] = None):
        self.broker = broker
        self.subject = subject
        self.cb = cb
        self.queue: Optional[asyncio.Queue] = None if cb else asyncio.Queue()

    async def next_msg(self, timeout: Optional[float] = None) -> NatsMessage:
        if self.queue is None:
            raise RuntimeError("next_msg() not available on callback-based subscriptions")
        return await asyncio.wait_for(self.queue.get(), timeout)

    async def unsubscribe(self):
        self.broker.remove_sub(self)


class MockNATSBroker:
    """Shared state for one NATS 'server'. Supports basic subject
    wildcards: '*' matches exactly one token, '>' matches the rest."""

    def __init__(self):
        self._subs: list[MockNATSSubscription] = []

    @staticmethod
    def _matches(pattern: str, subject: str) -> bool:
        p_tokens = pattern.split(".")
        s_tokens = subject.split(".")
        for i, pt in enumerate(p_tokens):
            if pt == ">":
                return True
            if i >= len(s_tokens) or (pt != "*" and pt != s_tokens[i]):
                return False
        return len(p_tokens) == len(s_tokens)

    def add_sub(self, sub: MockNATSSubscription):
        self._subs.append(sub)

    def remove_sub(self, sub: MockNATSSubscription):
        if sub in self._subs:
            self._subs.remove(sub)

    async def publish(self, subject: str, data: bytes):
        msg = NatsMessage(subject=subject, data=data)
        for sub in list(self._subs):
            if self._matches(sub.subject, subject):
                if sub.cb:
                    asyncio.create_task(sub.cb(msg))
                else:
                    await sub.queue.put(msg)


class MockNATSClient:
    """Drop-in replacement for the object returned by nats.connect()
    (subset used for demos: publish/subscribe/close)."""

    def __init__(self, broker: MockNATSBroker):
        self._broker = broker

    async def publish(self, subject: str, data: bytes | str):
        if isinstance(data, str):
            data = data.encode()
        await self._broker.publish(subject, data)

    async def subscribe(self, subject: str,
                         cb: Optional[Callable[[NatsMessage], Awaitable[None]]] = None) -> MockNATSSubscription:
        sub = MockNATSSubscription(self._broker, subject, cb)
        self._broker.add_sub(sub)
        return sub

    async def close(self):
        pass

    async def drain(self):
        pass


async def mock_nats_connect(broker: MockNATSBroker, **kwargs) -> MockNATSClient:
    """Mimics `await nats.connect(...)`."""
    return MockNATSClient(broker)


# ============================================================
# Demo
# ============================================================

async def _kafka_demo():
    broker = MockKafkaBroker()

    consumer = MockAIOKafkaConsumer("orders", broker=broker, group_id="demo-group")
    await consumer.start()

    producer = MockAIOKafkaProducer(broker)
    await producer.start()

    async def consume():
        async for msg in consumer:
            # print(f"[kafka] topic={msg.topic} value={msg.value}")
            if msg.value == "stop":
                break

    consume_task = asyncio.create_task(consume())

    await producer.send_and_wait("orders", {"id": 1, "item": "widget"})
    await producer.send_and_wait("orders", {"id": 2, "item": "gadget"})
    await producer.send_and_wait("orders", "stop")

    await consume_task
    await producer.stop()
    await consumer.stop()


async def _nats_demo():
    broker = MockNATSBroker()
    nc = await mock_nats_connect(broker)

    # pull-style subscription
    sub = await nc.subscribe("updates.*")

    async def consume():
        while True:
            msg = await sub.next_msg()
            # print(f"[nats] subject={msg.subject} data={msg.data.decode()}")
            if msg.data == b"stop":
                break

    consume_task = asyncio.create_task(consume())

    await nc.publish("updates.price", "price changed to 42")
    await nc.publish("updates.stock", "stock changed to 10")
    await nc.publish("updates.done", "stop")

    await consume_task
    await sub.unsubscribe()
    await nc.close()


async def main():
    # print("--- Kafka mock demo ---")
    await _kafka_demo()
    # print("\n--- NATS mock demo ---")
    await _nats_demo()


if __name__ == "__main__":
    asyncio.run(main())