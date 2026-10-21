import json
import asyncio
from collections import deque
from nats.aio.client import Client as NATS
from loguru import logger as log

class SLAMonitor:
    def __init__(self, nats_url="nats://localhost:4222"):
        self.nats_url = nats_url
        self.nc = NATS()
        
        # {intent_id: RanRFS dict}
        self.active_services = {}
        
        # {intent_id: deque([last_100_reports])}
        self.metrics_history = {}

        #{intent_id: current_handling_state}
        self.intent_states = {}
        self.MAX_HISTORY_PER_SERVICE = 100

    async def connect(self):
        try:
            await self.nc.connect(self.nats_url)
            log.info(f"SLAMonitor: Connected to NATS at {self.nats_url}")

            await self.nc.subscribe("service.telemetry.report", cb=self._on_metrics_received)
            await self.nc.subscribe("monitor.service.start", cb=self._on_service_register)
            await self.nc.subscribe("monitor.service.stop", cb=self._on_service_deregister)
            
            log.success("SLAMonitor: All NATS subscriptions active.")
        except Exception as e:
            log.error(f"SLAMonitor: Connection error: {e}")

    async def _on_service_register(self, msg):
        try:
            data = json.loads(msg.data.decode())
            intent_id = data.get("intent_id")
            rfs_ran = data.get("rfs_ran")

            if intent_id and rfs_ran:
                self.active_services[intent_id] = rfs_ran
                self.intent_states[intent_id] = None
                self.metrics_history[intent_id] = deque(maxlen=self.MAX_HISTORY_PER_SERVICE)
                log.success(f"SLAMonitor: Registered monitoring for {intent_id}. SLS: {rfs_ran}")
        except Exception as e:
            log.error(f"SLAMonitor: Registration error: {e}")

    async def _on_service_deregister(self, msg):
        data = json.loads(msg.data.decode())
        intent_id = data.get("intent_id")
        
        if intent_id in self.active_services:
            
            history_len = len(self.metrics_history.get(intent_id, []))
            log.warning(f"SLAMonitor: Stopping monitor for {intent_id}. Data points collected: {history_len}")
            
            del self.active_services[intent_id]
            del self.intent_states[intent_id]
            self.metrics_history.pop(intent_id, None)

    async def _on_metrics_received(self, msg):
        try:
            data = json.loads(msg.data.decode())
            intent_id = data.get("intent_id")
            metrics = data.get("metrics", {})
            timestamp = data.get("timestamp", str(asyncio.get_event_loop().time()))

            if intent_id not in self.active_services:
                return

            history_entry = {
                "timestamp": timestamp,
                "metrics": metrics
            }
            self.metrics_history[intent_id].append(history_entry)

            await self._check_sls_violations(intent_id, metrics)

        except Exception as e:
            log.error(f"SLAMonitor: Metrics analysis error: {e}")

    async def _check_sls_violations(self, intent_id, metrics):
        rfs = self.active_services[intent_id]
        violations = []

        # RanRFS fields; None means the intent does not constrain that metric.
        target_tp = rfs.get("throughput_mbps")
        target_lat = rfs.get("latency_ms")

        current_tp = metrics.get("throughput_mbps")
        current_lat = metrics.get("latency_ms")

        if target_tp is not None and current_tp is not None and current_tp < target_tp:
            violations.append(f"Throughput Violation: {current_tp}Mbps < {target_tp}Mbps")

        if target_lat is not None and current_lat is not None and current_lat > target_lat:
            violations.append(f"Latency Violation: {current_lat}ms > {target_lat}ms")

        new_state = "STATE_DEGRADES" if violations else "STATE_COMPLIES"
        
        old_state = self.intent_states.get(intent_id)

        if new_state != old_state:
            await self._notify_intent_gateway(intent_id, new_state)
            self.intent_states[intent_id] = new_state

        if violations:
            await self._trigger_fault_alert(intent_id, violations, metrics)

    async def _notify_intent_gateway(self, intent_id, state):
        status_update = {
            "message_type": "IntentStatusUpdate",
            "intent_id": intent_id,
            "current_handling_state": state
        }
        
        await self.nc.publish("intent.status.update", json.dumps(status_update).encode())
        
        if state == "STATE_DEGRADES":
            log.error(f"SLAMonitor -> IntentGateway: {intent_id} is DEGRADED!")
        else:
            log.success(f"SLAMonitor -> IntentGateway: {intent_id} is COMPLIANT.")
            
    async def _trigger_fault_alert(self, intent_id, violations, metrics):
        alert_payload = {
            "message_type": "SlsViolation",
            "intent_id": intent_id,
            "violations": violations,
            "current_metrics": metrics,
            "severity": "CRITICAL"
        }
        
        subject = f"sls.violation"
        await self.nc.publish(subject, json.dumps(alert_payload).encode())
        log.warning(f"SLAMonitor: SENT ALERT to {subject} -> {violations}")

    async def run(self):
        await self.connect()
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    fm = SLAMonitor()
    try:
        asyncio.run(fm.run())
    except KeyboardInterrupt:
        log.info("SLAMonitor: Shutting down...")