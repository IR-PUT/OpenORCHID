import asyncio
import json

from idmo_system.communication.api import UnityRanCapabilities
from idmo_system.domains.base_domain import BaseDomainBroker
from idmo_system.domains.behavior import RanBehavior
from loguru import logger

DEFAULT_RAN_CAPABILITIES = UnityRanCapabilities(
    maxNumberofUEs=100,
    nROperatingBands=["n78", "n258"],
    coverageAreaTAList=[(260, 1, 100)], # TAI - Tracking Area Identity (mcc, mnc, tac)
    positioning=True,
    core_links={"default_upf": 1000.0}
)


class RANDomainBroker(BaseDomainBroker):
    def __init__(self, domain_name=None, capabilities: UnityRanCapabilities = DEFAULT_RAN_CAPABILITIES,
                 behavior: RanBehavior | None = None, kafka_bootstrap="localhost:9092"):
        super().__init__(domain_type="ran", capabilities=capabilities.model_dump(),
                         domain_name=domain_name, kafka_bootstrap=kafka_bootstrap)
        self.behavior = behavior or RanBehavior()
        self._telemetry_tasks = {}

    async def handle_feasibility(self, data):
        max_bw = self.behavior.max_bandwidth_mhz
        requested_bw = data.get("rfs_ran", {}).get("bandwidth_mhz") or 0
        is_ok = requested_bw <= max_bw
        proposals = None if is_ok else {"bandwidth_mhz": max_bw}
        return is_ok, proposals

    async def handle_provisioning(self, data):
        return f"ran_slice_{data['intent_id']}"
    
    async def setup_telemetry(self, data, service_topic):
        interval = self.behavior.telemetry.interval_s
        intent_id = data.get("intent_id")
        
        logger.info(f"RAN: Starting telemetry for {intent_id}")
        task = asyncio.create_task(self._telemetry_loop(service_topic, interval, intent_id))
        self._telemetry_tasks[service_topic] = task

    async def _telemetry_loop(self, topic_name, interval, intent_id):
        while True:
            await asyncio.sleep(interval)
            
            metrics = {
                "throughput_mbps": self.behavior.telemetry.throughput_mbps,
                "latency_ms": self.behavior.telemetry.latency_ms
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