import asyncio
import re
from typing import NamedTuple
import sys
import types
from collections import defaultdict


class MockNewTopic:
    def __init__(self, name, num_partitions, replication_factor, topic_configs=None):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.topic_configs = topic_configs or {}


class MockTopicPartition(NamedTuple):
    topic: str
    partition: int


class MockConsumerRecord:
    def __init__(self, topic, key, value, offset):
        self.topic = topic
        self.key = key
        self.value = value
        self.partition = 0
        self.offset = offset


class _MockKafkaBroker:
    def __init__(self):
        self.topics = set()
        self.messages = defaultdict(list)   # topic -> list[(key, value)]
        self.consumers = set()

    def create_topic(self, name):
        self.topics.add(name)
        self.messages.setdefault(name, [])

    def delete_topic(self, name):
        self.topics.discard(name)
        self.messages.pop(name, None)

    def list_topics(self):
        return sorted(self.topics)

    def register_consumer(self, consumer):
        self.consumers.add(consumer)

    def unregister_consumer(self, consumer):
        self.consumers.discard(consumer)

    def publish(self, topic, key, value):
        if topic not in self.topics:
            self.create_topic(topic)

        offset = len(self.messages[topic])
        self.messages[topic].append((key, value))

        # Fan-out to every matching subscription.
        for consumer in list(self.consumers):
            consumer._deliver_if_match(topic, key, value, offset)

    def replay_for_consumer(self, consumer):
        # With auto_offset_reset=earliest, replay the topic history.
        if getattr(consumer, "auto_offset_reset", None) != "earliest":
            return

        for topic in sorted(self.topics):
            if consumer._matches(topic):
                start_offset = consumer._offsets[topic]
                for offset in range(start_offset, len(self.messages[topic])):
                    key, value = self.messages[topic][offset]
                    consumer._queue.put_nowait(MockConsumerRecord(topic, key, value, offset))
                consumer._offsets[topic] = len(self.messages[topic])


# Single in-process "cluster" shared by every mock client.
_BROKER = _MockKafkaBroker()


class MockAIOKafkaAdminClient:
    def __init__(self, bootstrap_servers, loop=None, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self._broker = _BROKER

    async def start(self):
        await asyncio.sleep(0)

    async def stop(self):
        await asyncio.sleep(0)

    async def close(self):
        await self.stop()

    async def create_topics(self, new_topics, timeout_ms=None, validate_only=False):
        for topic in new_topics:
            name = getattr(topic, "name", str(topic))
            self._broker.create_topic(name)
        await asyncio.sleep(0)
        return None

    async def delete_topics(self, topics, timeout_ms=None):
        for topic in topics:
            self._broker.delete_topic(topic)
        await asyncio.sleep(0)
        return None

    async def list_topics(self):
        await asyncio.sleep(0)
        return self._broker.list_topics()


class MockAIOKafkaProducer:
    def __init__(self, bootstrap_servers, loop=None, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self._broker = _BROKER

    async def start(self):
        await asyncio.sleep(0)

    async def stop(self):
        await asyncio.sleep(0)

    async def send_and_wait(self, topic, value, key=None, partition=None, timestamp=None):
        self._broker.publish(topic, key, value)

        class MockRecordMetadata:
            def __init__(self, t):
                self.topic = t
                self.partition = 0
                self.offset = 0

        await asyncio.sleep(0)
        return MockRecordMetadata(topic)

    async def send(self, topic, value=None, key=None, partition=None, timestamp=None):
        return await self.send_and_wait(topic, value, key, partition, timestamp)


class _MockConsumerClient:
    async def force_metadata_update(self):
        await asyncio.sleep(0)


class MockAIOKafkaConsumer:
    def __init__(
        self,
        *topics,
        bootstrap_servers,
        group_id=None,
        auto_offset_reset="latest",
        loop=None,
        **kwargs
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset

        self._broker = _BROKER
        self._queue = asyncio.Queue()
        self._is_closed = True

        self._topics = set(topics)
        self._pattern = None
        self._offsets = defaultdict(int)

        self._client = _MockConsumerClient()

    async def start(self):
        self._is_closed = False
        self._broker.register_consumer(self)
        self._broker.replay_for_consumer(self)
        await asyncio.sleep(0)

    async def stop(self):
        self._is_closed = True
        self._broker.unregister_consumer(self)
        await asyncio.sleep(0)

    def subscribe(self, topics=(), pattern=None, listener=None):
        if pattern:
            self._pattern = re.compile(pattern)
            self._topics = set()
        else:
            self._pattern = None
            self._topics = set(topics)

        # Make sure subscribed topics exist.
        for t in self._topics:
            self._broker.create_topic(t)

        # After a subscription change, replay history for earliest.
        self._broker.replay_for_consumer(self)

    def _matches(self, topic):
        if self._pattern is not None:
            return bool(self._pattern.match(topic))
        return topic in self._topics

    def _deliver_if_match(self, topic, key, value, offset):
        if not self._matches(topic):
            return

        # With earliest, skip messages already delivered by replay.
        if self.auto_offset_reset == "earliest":
            if offset < self._offsets[topic]:
                return

        self._queue.put_nowait(MockConsumerRecord(topic, key, value, offset))
        self._offsets[topic] = max(self._offsets[topic], offset + 1)

    async def getone(self):
        if self._is_closed:
            raise StopAsyncIteration
        return await self._queue.get()

    async def commit(self, offsets=None):
        await asyncio.sleep(0)

    async def get_many(self, timeout_ms=0, max_records=None):
        # API compatibility only; the adapter ignores the result.
        if timeout_ms and timeout_ms > 0:
            await asyncio.sleep(timeout_ms / 1000.0)
        return {}

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._is_closed:
            raise StopAsyncIteration
        return await self.getone()


def reset():
    """Forget all topics, messages and consumers (e.g. between two scenario runs in one process)."""
    _BROKER.__init__()


def install():
    """Replace the aiokafka modules in sys.modules with in-process mocks.

    Must be called before any module that imports aiokafka is imported.
    """
    mock_main_mod = types.ModuleType("aiokafka")
    mock_main_mod.AIOKafkaProducer = MockAIOKafkaProducer
    mock_main_mod.AIOKafkaConsumer = MockAIOKafkaConsumer
    mock_main_mod.TopicPartition = MockTopicPartition

    mock_admin_mod = types.ModuleType("aiokafka.admin")
    mock_admin_mod.AIOKafkaAdminClient = MockAIOKafkaAdminClient
    mock_admin_mod.NewTopic = MockNewTopic

    mock_main_mod.admin = mock_admin_mod

    sys.modules["aiokafka"] = mock_main_mod
    sys.modules["aiokafka.admin"] = mock_admin_mod
