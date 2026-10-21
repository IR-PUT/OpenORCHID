import asyncio
import json
import random

from idmo_system.communication.api import UnityRanCapabilities
from idmo_system.domains.base_domain import BaseDomainBroker, FeasibilityResult
from idmo_system.domains.behavior import DomainEvent, RanBehavior
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
                 behavior: RanBehavior | None = None, kafka_bootstrap="localhost:9092", seed=None):
        super().__init__(domain_type="ran", capabilities=capabilities.model_dump(),
                         domain_name=domain_name, kafka_bootstrap=kafka_bootstrap)
        self.behavior = behavior or RanBehavior()
        self._telemetry_tasks = {}
        self._rng = random.Random(seed) # "random" telemetry profile
        # Multipliers set by "degrade" / "restore" events
        self._throughput_factor = 1.0
        self._latency_factor = 1.0

    async def apply_event(self, event: DomainEvent):
        if event.action == "degrade":
            logger.warning(f"RAN {self.domain_name}: EVENT degrade (throughput x{event.throughput_factor}, "
                           f"latency x{event.latency_factor})")
            self._throughput_factor = event.throughput_factor
            self._latency_factor = event.latency_factor
        elif event.action == "restore":
            logger.warning(f"RAN {self.domain_name}: EVENT restore")
            self._throughput_factor = self._latency_factor = 1.0
        else:
            await super().apply_event(event)

    def compute_metrics(self, report_number: int) -> dict:
        """Telemetry of the n-th report (0-based) of a service, according to the configured profile."""
        t = self.behavior.telemetry
        throughput, latency = t.throughput_mbps, t.latency_ms

        if t.profile == "random":
            throughput *= 1 + self._rng.uniform(-t.jitter, t.jitter)
            latency *= 1 + self._rng.uniform(-t.jitter, t.jitter)
        elif t.profile == "degrading":
            quality = (1 - t.degradation_per_report) ** report_number
            throughput *= quality
            latency /= quality

        return {
            "throughput_mbps": round(throughput * self._throughput_factor, 2),
            "latency_ms": round(latency * self._latency_factor, 2)
        }

    async def handle_feasibility(self, data):
        rfs = data.get("rfs_ran") or {}
        reasons = []

        tac = rfs.get("tac")
        if tac is not None and tac not in [ta[2] for ta in self.capabilities["coverageAreaTAList"]]:
            reasons.append(f"TA {tac} not covered")

        free_bw = self.behavior.max_bandwidth_mhz - self.reserved("bandwidth_mhz")
        requested_bw = rfs.get("bandwidth_mhz") or 0
        if requested_bw > free_bw:
            reasons.append(f"{requested_bw:g} MHz requested, {free_bw:g} MHz available")

        free_ues = self.capabilities["maxNumberofUEs"] - self.reserved("num_ues")
        requested_ues = rfs.get("num_ues") or 0
        if requested_ues > free_ues:
            reasons.append(f"{requested_ues} UEs requested, {free_ues:g} available")

        if reasons:
            return FeasibilityResult(False, proposals={"bandwidth_mhz": free_bw, "num_ues": free_ues},
                                     reason="; ".join(reasons))
        return FeasibilityResult(True)

    def resources_for(self, data):
        rfs = data.get("rfs_ran") or {}
        return {"bandwidth_mhz": rfs.get("bandwidth_mhz") or 0, "num_ues": rfs.get("num_ues") or 0}

    async def handle_provisioning(self, data):
        return f"ran_slice_{data['intent_id']}"
    
    async def setup_telemetry(self, data, service_topic):
        interval = self.behavior.telemetry.interval_s
        intent_id = data.get("intent_id")
        
        logger.info(f"RAN: Starting telemetry for {intent_id}")
        task = self._spawn(self._telemetry_loop(service_topic, interval, intent_id))
        self._telemetry_tasks[service_topic] = task

    async def _telemetry_loop(self, topic_name, interval, intent_id):
        report_number = 0
        while True:
            await asyncio.sleep(interval)

            metrics = self.compute_metrics(report_number)
            report_number += 1
            
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