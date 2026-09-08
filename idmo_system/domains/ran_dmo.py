import asyncio
import json

from idmo_system.domains.base_domain import BaseDomainBroker

from idmo_system.domains.base_domain import BaseDomainBroker
from loguru import logger

class RANDomainBroker(BaseDomainBroker):
    def __init__(self, kafka_bootstrap="localhost:9092"):
        super().__init__(domain_type="ran", kafka_bootstrap=kafka_bootstrap)
        self._telemetry_tasks = {}

    async def start(self):
        """
        Inicjalizuje brokera z danymi zgodnymi z modelem UnityRanCapabilities.
        """
        ran_caps = {
            "maxNumberofUEs": 100,
            "nROperatingBands": ["n78", "n258"],
            # TAI - Tracking Area Identity (mcc, mnc, tac) 
            "coverageAreaTAList": [[260, 1, 100]], 
            "positioning": True,
            "core_links": {"default_upf": 1000.0}
        }
        
        await self.run(capabilities=ran_caps)

    async def handle_feasibility(self, data):
        requested_bw = data.get("rfs_ran", {}).get("bandwidth", 0)
        is_ok = requested_bw <= 4000
        proposals = None if is_ok else {"bandwidth": 4000}
        return is_ok, proposals

    async def handle_provisioning(self, data):
        return f"ran_slice_{data['intent_id']}"
    
    async def setup_telemetry(self, data, service_topic):
        interval = data.get("telemetry_interval", 5.0)
        intent_id = data.get("intent_id")
        
        logger.info(f"RAN: Starting telemetry for {intent_id}")
        task = asyncio.create_task(self._telemetry_loop(service_topic, interval, intent_id))
        self._telemetry_tasks[service_topic] = task

    async def _telemetry_loop(self, topic_name, interval, intent_id):
        while True:
            await asyncio.sleep(interval)
            
            metrics = {
                "throughput_mbps": 85.5,
                "latency_ms": 12.0
            }
            
            payload = {
                "message_type": "ServiceTelemetryReport",
                "intent_id": intent_id,
                "metrics": metrics
            }
            await self.producer.send_and_wait(topic_name, json.dumps(payload).encode())

    async def _process_termination(self, data, topic_name):
        intent_id = data.get("intent_id")

        if topic_name in self._telemetry_tasks:
            logger.warning(f"RAN: Terminating metrics task for {intent_id}...")
            task = self._telemetry_tasks[topic_name]
            
            task.cancel()
            try:
                await task 
            except asyncio.CancelledError:
                logger.debug(f"RAN: Metrics task for {intent_id} successfully cancelled.")
            
            self._telemetry_tasks.pop(topic_name)

        await super()._process_termination(data, topic_name)