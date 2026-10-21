"""In-process replacements for Kafka, NATS and the LLM, so the system can run without external infrastructure.

Kafka and NATS mocks patch sys.modules, so install() has to be called before importing
any idmo_system module that uses aiokafka or nats.
"""


def install(kafka: bool = True, nats: bool = True, llm: bool = True):
    if kafka:
        from idmo_system.mocks import kafka_mock
        kafka_mock.install()
    if nats:
        from idmo_system.mocks import nats_mock
        nats_mock.install()
    if llm:
        from idmo_system.mocks import llm_mock
        llm_mock.install()
