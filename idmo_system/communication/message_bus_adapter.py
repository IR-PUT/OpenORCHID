import json
import asyncio
from nats.aio.client import Client as NATS
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from loguru import logger as log

from idmo_system.communication.api import DomainHealthMessage, DomainRegistrationRequest, DomainRegistrationResponse

class MessageBusAdapter:
    def __init__(self, kafka_bootstrap="localhost:9092", nats_url="nats://localhost:4222"):
        self.nats_url = nats_url
        self.kafka_bootstrap = kafka_bootstrap
        self.nc = NATS()
        self.producer = None
        self.consumer = None

    async def connect(self):
        """Connects to NATS (Internal) and Kafka (External)."""
        await self.nc.connect(self.nats_url)
        self.producer = AIOKafkaProducer(bootstrap_servers=self.kafka_bootstrap)
        await self.producer.start()
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.kafka_bootstrap,
            group_id="idmo_mba_group",
            metadata_max_age_ms=5000 
        )
        await self.consumer.start()

        await self._ensure_bootstrap_topics()

        regex_pattern = r"^unity6g\.(domain\.registration|from|service)\..*"
        
        self.consumer.subscribe(pattern=regex_pattern)
        
        log.info(f"MBA: Pattern subscription active: {regex_pattern}")
        log.info("MBA: Connected to NATS and Kafka. Subscribed to global topics.")


    async def _ensure_bootstrap_topics(self):
        """Creates the initial topics needed for any domain to even talk to us."""
        admin = AIOKafkaAdminClient(bootstrap_servers=self.kafka_bootstrap)
        await admin.start()
        
        bootstrap_topics = [
            NewTopic(name="unity6g.domain.registration.request", num_partitions=1, replication_factor=1),
            NewTopic(name="unity6g.domain.registration.response", num_partitions=1, replication_factor=1)
        ]
        
        try:
            await admin.create_topics(new_topics=bootstrap_topics)
            log.info("Registry: Bootstrap Kafka topics created successfully.")
        except Exception as e:
            log.debug(f"Registry: Bootstrap topics already exist: {e}")
        finally:
            await admin.close()

    async def kafka_to_nats_bridge(self):
        """
        Translates Incoming Kafka messages (External) to Internal NATS events.
        """
        
        async for msg in self.consumer:
            log.debug(f"MBA RECEIVED: Topic={msg.topic}, Offset={msg.offset}")
            try:
                raw_data = json.loads(msg.value.decode())
                topic = msg.topic
                
                msg_type = raw_data.get("message_type")

                # 1. Domain Registration Request
                if topic == DomainRegistrationRequest.kafka_topic:
                    log.info(f"MBA: Relaying Registration Request from {raw_data.get('source_module')}")
                    await self.nc.publish("domain.registration.request", msg.value)

                # 3. Handling Dynamic Topics (Health/Telemetry)
                elif topic.startswith("unity6g.from."):
                    if msg_type == DomainHealthMessage.__name__:
                        await self.nc.publish("domain.heartbeat", msg.value)
                    elif msg_type == "FeasibilityCheckResponse":
                        await self.nc.publish("feasibility.check.response", msg.value)
                elif topic.startswith("unity6g.service."):        
                    if msg_type == "ServiceChangeStatus":
                        log.info(f"MBA: Service status changed confirmed on topic {topic}. Notifying Saga.")
                        await self.nc.publish("service.change.status", msg.value)
                    elif msg_type == "ServiceTelemetryReport":
                        log.info(f"MBA: Telemetry report on topic {topic}. Notifying SLAMonitor.")
                        await self.nc.publish("service.telemetry.report", msg.value)

            except Exception as e:
                log.error(f"MBA Error during Kafka-to-NATS translation: {e}")

    async def nats_to_kafka_bridge(self):
        registration_ack_sub = await self.nc.subscribe("domain.registration.ack")
        feasibility_sub = await self.nc.subscribe("feasibility.check.request")
        provision_sub = await self.nc.subscribe("service.order.request.dispatch")
        termination_sub = await self.nc.subscribe("service.order.terminate")
        clean_topics_sub = await self.nc.subscribe("clean.topics")
        
        async def handle_registration_acks():
            async for msg in registration_ack_sub.messages:
                try:
                    data = json.loads(msg.data.decode())
                    domain_id = data.get("domain_unique_id")
                    if domain_id:
                        to_topic = f"unity6g.to.{domain_id}"
                        from_topic = f"unity6g.from.{domain_id}"
                        
                        log.info(f"MBA: Registration ACK detected. Creating infra for {domain_id}")
                        await self.register_new_topic(to_topic)
                        await self.register_new_topic(from_topic)

                    topic = DomainRegistrationResponse.kafka_topic
                    await self.producer.send_and_wait(topic, msg.data)
                    log.success(f"MBA: Relayed Registration Response from NATS to Kafka topic: {topic}")
                except Exception as e:
                    log.error(f"MBA: Error relaying registration ack: {e}")

        async def handle_feasibility():
            async for msg in feasibility_sub.messages:
                try:
                    data = json.loads(msg.data.decode())
                    log.info(f"MBA: Starting Feasibility Phase for Intent {data.get('intent_id')}")
                    await self._route_to_kafka_domain_specific_topic(data, msg.data)
                except Exception as e:
                    log.error(f"MBA: Feasibility routing error: {e}")

        async def handle_provisioning():
            async for msg in provision_sub.messages:
                try:
                    data = json.loads(msg.data.decode())
                    service_topic = data.get("service_topic")

                    if service_topic:
                        await self.register_new_topic(service_topic)
                        await self.consumer._client.force_metadata_update()
                        await asyncio.sleep(1.0)
                    
                    log.info(f"MBA: Starting Commissioning Phase for Intent {data.get('intent_id')}")
                    await self._route_to_kafka_domain_specific_topic(data, msg.data)
                except Exception as e:
                    log.error(f"MBA: Provisioning routing error: {e}")

        async def handle_termination():
            async for msg in termination_sub.messages:
                try:
                    data = json.loads(msg.data.decode())
                    service_topic = data.get("service_topic")
                    intent_id = data.get("intent_id")
                    log.warning(f"MBA: Relaying Termination Request for Intent {intent_id}")
                    
                    if service_topic:
                        log.warning(f"MBA: Relaying Termination Request for {intent_id} via SERVICE TOPIC: {service_topic}")
                        
                        await self.producer.send_and_wait(service_topic, msg.data)
                    else:
                        log.error(f"MBA: Cannot terminate {intent_id} - missing service_topic in message!")
                except Exception as e:
                    log.error(f"MBA: Termination routing error: {e}")

        async def handle_domain_infrastructure_cleanup():
            async for msg in clean_topics_sub.messages:
                try:
                    data = json.loads(msg.data.decode())
                    topics = data.get("topics", [])
                    admin_client = AIOKafkaAdminClient(bootstrap_servers=self.kafka_bootstrap)
                    await admin_client.start()
                    try:
                        await admin_client.delete_topics(topics)
                        log.info(f"MBA: Cleaned up topics: {topics}")
                    finally:
                        await admin_client.close()
                except Exception as e:
                    log.error(f"MBA: Infrastructure cleanup error: {e}")

        await asyncio.gather(handle_registration_acks(), 
                             handle_feasibility(), 
                             handle_provisioning(),
                             handle_termination(),
                             handle_domain_infrastructure_cleanup())

    async def _route_to_kafka_domain_specific_topic(self, data, raw_bytes):
        """
        Universal internal helper to forward data to the correct Kafka topics.
        """
        targets = data.get("target_domains", {})
        
        for domain_type, domain_id in targets.items():
            topic = f"unity6g.to.{domain_id}"
            await self.producer.send_and_wait(topic, raw_bytes)
            log.debug(f"MBA: Message forwarded to Kafka topic: {topic}")

    async def register_new_topic(self, topic_name):
        """Ensures the topic exists in Kafka and then subscribes."""
        admin_client = AIOKafkaAdminClient(bootstrap_servers=self.kafka_bootstrap)
        await admin_client.start()
        try:
            new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            await admin_client.create_topics([new_topic])
            log.info(f"MBA: Dynamic topic {topic_name} created.")

            await self.consumer.get_many(timeout_ms=0)
            await asyncio.sleep(1.0)
        except Exception as e:
            log.debug(f"MBA: Topic {topic_name} already exists.")
        finally:
            await admin_client.close()

    async def start(self):
        await self.connect()
        await asyncio.gather(
            self.kafka_to_nats_bridge(),
            self.nats_to_kafka_bridge()
        )

if __name__ == "__main__":
    mba = MessageBusAdapter()
    asyncio.run(mba.start())