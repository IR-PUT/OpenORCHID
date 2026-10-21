import uuid
import sys
import asyncio
import types
from collections import defaultdict


class MockMessage:
    def __init__(self, data, subject="", reply=None):
        self.data = data
        self.subject = subject
        self.reply = reply or uuid.uuid4().hex


class MockMessages:
    def __init__(self):
        self.messages = []
        self.index = 0
        self._is_closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        while not self._is_closed:

            if self.index < len(self.messages):
                message = self.messages[self.index]
                self.index += 1
                return message
            

            await asyncio.sleep(0.05)

        raise StopAsyncIteration

class MockSubscription:
    def __init__(self, callback=None):
        self.messages = MockMessages()
        self.callback = callback


class MockNATSClient:

    storage = defaultdict(list) 

    def __init__(self):
        pass
        # print("[Mock NATS] Client initialization")

    async def connect(self, server_url):
        # print(f"[Mock NATS] Connected to server: {server_url}")
        await asyncio.sleep(0.01)

    async def close(self):
        # print("[Mock NATS] NATS connection closed")
        await asyncio.sleep(0.01)

    async def publish(self, subject, payload):
        delivered = False

        # Deliver to all subscriptions that match this subject, including wildcards.
        for subscription_subject, subscriptions in list(MockNATSClient.storage.items()):
            if not self._subject_matches(subscription_subject, subject):
                continue

            for subscription in list(subscriptions):
                delivered = True
                # print(type(payload), payload)
                msg = MockMessage(payload, subject=subject)
                if subscription.callback is not None:
                    # Do not block fan-out on one callback subscriber.
                    asyncio.create_task(subscription.callback(msg))
                else:
                    subscription.messages.messages.append(msg)

        # Preserve basic request/reply behavior when no active subscriber consumed this publish.
        if not delivered:
            sub = MockSubscription()
            sub.messages.messages.append(MockMessage(payload, subject=subject))
            MockNATSClient.storage[subject].append(sub) # for req-reply


    async def subscribe(self, subject, cb=None):
        # print(f"[Mock NATS] Subscribed to subject '{subject}'")
        sub = MockSubscription(callback=cb)
        MockNATSClient.storage[subject].append(sub)
        return sub

    async def request(self, subject, payload, timeout=2.0):
        reply_subject = f"_INBOX.{uuid.uuid4().hex}"
        inbox_sub = MockSubscription()
        MockNATSClient.storage[reply_subject].append(inbox_sub)

        try:
            target_callback = self._select_request_handler(subject)
            if target_callback is None:
                return MockMessage(b'{}', subject=subject)

            request_message = MockMessage(payload, subject=subject, reply=reply_subject)
            await target_callback(request_message)

            # Wait for the business handler to publish a reply to the private inbox.
            return await asyncio.wait_for(inbox_sub.messages.__anext__(), timeout=timeout)
        except asyncio.TimeoutError:
            return MockMessage(b'{}', subject=subject)
        finally:
            if inbox_sub in MockNATSClient.storage.get(reply_subject, []):
                MockNATSClient.storage[reply_subject].remove(inbox_sub)
            if not MockNATSClient.storage.get(reply_subject):
                MockNATSClient.storage.pop(reply_subject, None)

    def _select_request_handler(self, subject):
        # Prefer exact subject callbacks (business receivers) over wildcard monitors.
        for subscription in MockNATSClient.storage.get(subject, []):
            if subscription.callback is not None:
                return subscription.callback

        # Fallback to wildcard callbacks if no exact callback exists.
        for subscription_subject, subscriptions in list(MockNATSClient.storage.items()):
            if subscription_subject == subject:
                continue
            if not self._subject_matches(subscription_subject, subject):
                continue
            if not self._is_wildcard_subject(subscription_subject):
                continue

            for subscription in subscriptions:
                if subscription.callback is not None:
                    return subscription.callback

        return None

    @staticmethod
    def _is_wildcard_subject(subject):
        return "*" in subject or ">" in subject

    @staticmethod
    def _subject_matches(subscription_subject, published_subject):
        # Support NATS-like wildcards used in this project.
        if subscription_subject == ">":
            return True

        sub_tokens = subscription_subject.split('.')
        pub_tokens = published_subject.split('.')

        i = 0
        while i < len(sub_tokens):
            token = sub_tokens[i]

            if token == ">":
                return True

            if i >= len(pub_tokens):
                return False

            if token != "*" and token != pub_tokens[i]:
                return False

            i += 1

        return i == len(pub_tokens)


async def _mock_connect(servers, **kwargs):
    client = MockNATSClient()
    await client.connect(servers)
    return client


def install():
    """Replace the nats modules in sys.modules with in-process mocks.

    Must be called before any module that imports nats is imported.
    """
    mock_nats = types.ModuleType('nats')
    mock_nats_aio = types.ModuleType('nats.aio')
    mock_nats_aio_client = types.ModuleType('nats.aio.client')

    mock_nats_aio_client.Client = MockNATSClient
    mock_nats_aio.client = mock_nats_aio_client
    mock_nats.aio = mock_nats_aio
    mock_nats.connect = _mock_connect
    mock_nats.NATS = MockNATSClient

    sys.modules['nats'] = mock_nats
    sys.modules['nats.aio'] = mock_nats_aio
    sys.modules['nats.aio.client'] = mock_nats_aio_client
