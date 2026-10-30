# All tests run with the in-process Kafka/NATS mocks. They replace aiokafka/nats in sys.modules,
# so they must be installed before any idmo_system module that imports them.
from idmo_system import mocks

mocks.install(llm=False)
