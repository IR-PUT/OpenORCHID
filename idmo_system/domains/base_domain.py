import json
import asyncio
import uuid
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from loguru import logger as log

class BaseDomainBroker:
    def __init__(self, domain_type, kafka_bootstrap="localhost:9092"):
        self._domain_type = domain_type
        self.kafka_bootstrap = kafka_bootstrap
        
        self._domain_unique_id = None
        self._topic_idmo_to_domain = None
        self._topic_domain_to_idmo = None
        self._service_listeners = {}
        
        self._temporary_domain_id = f"temp_{domain_type}_{uuid.uuid4().hex[:6]}"
        self.producer = None

    async def run(self, capabilities):
        self.producer = AIOKafkaProducer(bootstrap_servers=self.kafka_bootstrap)
        await self.producer.start()

        asyncio.create_task(self._subscribe_to_registration_responses())
        
        await asyncio.sleep(1.0) 

        log.info(f'Sending registration request for {self._domain_type}...')
        reg_msg = {
            "message_type": "DomainRegistrationRequest",
            "source_module": self._temporary_domain_id,
            "target_module": 'IDMO',
            "domain_name": f"Domain_{self._domain_type}",
            "domain_type": self._domain_type,
            "capabilities": capabilities
        }
        
        await self.producer.send_and_wait(
            "unity6g.domain.registration.request", 
            json.dumps(reg_msg).encode()
        )
        log.info('Sent domain registration request. Waiting for callback...')

        while True:
            await asyncio.sleep(3600)

    async def _subscribe_to_registration_responses(self):
        consumer = AIOKafkaConsumer(
            "unity6g.domain.registration.response",
            bootstrap_servers=self.kafka_bootstrap,
            group_id=f"group_{self._temporary_domain_id}",
            auto_offset_reset='earliest' # Gwarancja, że nie przegapimy odpowiedzi
        )
        await consumer.start()
        try:
            async for msg in consumer:
                raw_message = json.loads(msg.value.decode())
                
                if raw_message.get("target_module") == self._temporary_domain_id:
                    await self.receive_domain_registration_response(raw_message)
                    break 
        finally:
            await consumer.stop()

    async def receive_domain_registration_response(self, raw_message: dict) -> None:
        log.info(f'Received domain registration response: {raw_message}')

        if raw_message.get("status"):
            log.success(f"Successfully registered domain with ID {raw_message['domain_unique_id']}.")
            
            self._domain_unique_id = raw_message["domain_unique_id"]
            self._topic_idmo_to_domain = raw_message["topic_idmo_to_domain"]
            self._topic_domain_to_idmo = raw_message["topic_domain_to_idmo"]

            self._heartbeat_interval = raw_message.get("heartbeat_interval_s", 30.0)
            # Start command listener immediately to avoid missing early feasibility requests.
            asyncio.create_task(self._subscribe_to_direct_commands())
            asyncio.create_task(self._heartbeat_loop())
        else:
            log.error(f"Failed to register domain: {raw_message.get('message')}")

    async def _heartbeat_loop(self):
        log.info(f"Heartbeat loop started. Interval: {self._heartbeat_interval}s")
        while self._domain_unique_id:
            try:
                await self.send_heartbeat()
                await asyncio.sleep(self._heartbeat_interval)
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
                await asyncio.sleep(5)

    async def send_heartbeat(self) -> None:
        heartbeat_msg = {
            "message_type": "DomainHealthMessage",
            "source_module": self._domain_unique_id,
            "target_module": "IDMO",
            "status": True
        }

        await self.producer.send_and_wait(
            self._topic_domain_to_idmo, 
            json.dumps(heartbeat_msg).encode()
        )
        log.debug(f"Heartbeat sent from {self._domain_unique_id}")

    async def _subscribe_to_direct_commands(self):
        consumer = AIOKafkaConsumer(
            self._topic_idmo_to_domain,
            bootstrap_servers=self.kafka_bootstrap,
            group_id=f"group_{self._domain_unique_id}"
        )
        await consumer.start()
        log.info(f'Subscribed to direct topic {self._topic_idmo_to_domain}')
        
        async for msg in consumer:
            raw_message = json.loads(msg.value.decode())
            await self.receive_direct_message_from_idmo(raw_message)

    async def receive_direct_message_from_idmo(self, raw_message: dict) -> None:
        
        msg_type = raw_message.get("message_type")
        intent_id = raw_message.get("intent_id")
        service_topic = raw_message.get("service_topic")

        log.info(f"Received {msg_type} from IDMO for Intent {intent_id}")

        if msg_type == "FeasibilityCheckRequest":
            await self._process_feasibility(raw_message)
        elif msg_type == "ServiceOrderRequestDispatch": #
            await self._process_provisioning(raw_message, service_topic)

    async def _process_feasibility(self, data):
        result, proposals = await self.handle_feasibility(data)
        response = {
            "message_type": "FeasibilityCheckResponse",
            "intent_id": data.get("intent_id"),
            "domain_type": self._domain_type,
            "status": result,
            "proposals": proposals,
            "source_module": self._domain_unique_id,
            "target_module": "IDMO"
        }
        await self.producer.send_and_wait(self._topic_domain_to_idmo, 
                                          json.dumps(response).encode())
        log.info(f"Sent feasibility response to {self._topic_domain_to_idmo}")

    async def _process_provisioning(self, data, service_topic):
        resource_id = await self.handle_provisioning(data)
        activation_report = {
            "message_type": "ServiceChangeStatus",
            "intent_id": data.get("intent_id"),
            "status": "ACTIVE",
            "resource_id": resource_id,
            "domain_id": self._domain_unique_id,
            "source_module": self._domain_unique_id,
            "target_module": "IDMO"
        }
        await self.producer.send_and_wait(service_topic, 
                                          json.dumps(activation_report).encode())
        log.success(f"Activation confirmed on {service_topic}")

        if service_topic not in self._service_listeners:
            log.info(f"Broker: Starting dedicated listener for NEW service: {service_topic}")
            task = asyncio.create_task(self._listen_to_service_topic(service_topic))
            self._service_listeners[service_topic] = task

        await self.setup_telemetry(data, service_topic)

    async def setup_telemetry(self, data, service_topic):
        pass

    async def _listen_to_service_topic(self, topic_name):
        consumer = AIOKafkaConsumer(
            topic_name,
            bootstrap_servers=self.kafka_bootstrap,
            group_id=f"group_{self._domain_unique_id}_{topic_name}"
        )
        await consumer.start()
        
        try:
            log.info(f"Broker: Service listener for {topic_name} is active and waiting for commands...")
            async for msg in consumer:
                raw_message = json.loads(msg.value.decode())
                msg_type = raw_message.get("message_type")
                
                log.info(f"Broker: Received {msg_type} on service topic {topic_name}")

                if msg_type == "ServiceTermination":
                    await self._process_termination(raw_message, topic_name)
                    break 

                else:
                    pass
                    # log.warning(f"Broker: Unknown message type {msg_type} on service topic.")

        except Exception as e:
            log.error(f"Critical error in listener for {topic_name}: {e}")
        finally:
            await consumer.stop()
            self._service_listeners.pop(topic_name, None)
            log.info(f"Broker: Listener for {topic_name} permanently closed.")

    async def _process_termination(self, data, topic_name):
        intent_id = data.get("intent_id")
          
        log.warning(f"Domain {self._domain_unique_id}: Terminating service {intent_id}...")
        
        termination_report = {
            "message_type": "ServiceChangeStatus",
            "intent_id": intent_id,
            "status": "DEACTIVATED", 
            "domain_id": self._domain_unique_id,
            "source_module": self._domain_unique_id,
            "target_module": "IDMO"
        }

        await self.producer.send_and_wait(
            topic_name, 
            json.dumps(termination_report).encode()
        )
        log.success(f"Termination confirmed for {intent_id}. Sending DEACTIVATED status.")

    async def handle_feasibility(self, data): raise NotImplementedError
    async def handle_provisioning(self, data): raise NotImplementedError