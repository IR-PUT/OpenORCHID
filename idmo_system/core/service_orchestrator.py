import json
import random
import asyncio
from functools import partial

import nats
from loguru import logger as log

from idmo_system.core.domain_selection import select_candidates

DEFAULT_SERVICE_DURATION_S = 60.0


class ServiceSaga:
    """Represents the lifecycle of a single Service Intent (Saga Instance)"""
    def __init__(self, intent_id, rfs_ran, rfs_core, nats_client, select_candidates,
                 max_candidates=3, feasibility_timeout_s=5.0):
        self.intent_id = intent_id
        self.rfs_ran = rfs_ran
        self.rfs_core = rfs_core
        self.nc = nats_client
        self.state = "PREPARATION"
        self.feasibility_results = {"ran": None, "core": None}
        self.provisioning_results = {"ran": None, "core": None}
        self.termination_results = {"ran": None, "core": None}
        self.selected_domains = {}
        self.service_topic = None # Initialized during provision_service
        self.duration = rfs_ran.get("duration_s") or DEFAULT_SERVICE_DURATION_S

        # Preparation phase: ranked (RAN, CORE) candidates tried one after another.
        self._select_candidates = select_candidates # (domains, rfs_ran, rfs_core) -> list[Candidate]
        self.max_candidates = max_candidates
        self.feasibility_timeout_s = feasibility_timeout_s
        self.candidates = []
        self.attempt = 0
        self.attempts = [] # History: one entry per tried candidate

        self.fault_count = 0
        self.MAX_FAULT_THRESHOLD = 3

        # Inventory query can race with domain registration startup.
        self.inventory_retry_count = 6
        self.inventory_retry_delay_s = 0.5

    async def start(self):
        log.info(f"Saga-{self.intent_id}: Starting Preparation Phase (Feasibility Check)")
        await self.run_feasibility_check()

    async def run_feasibility_check(self):
        """Step 1: Preparation Phase. Query Registry, rank candidate domains, then query them one by one."""
        try:
            # 1. Query Registry for available domains
            log.info(f"Saga-{self.intent_id}: Querying Domain Registry...")
            inventory = await self._query_inventory_with_retry()
            domains = inventory.get("domains", {})

            # 2. Filter and rank (RAN, CORE) pairs
            self.candidates = self._select_candidates(domains, self.rfs_ran, self.rfs_core)[:self.max_candidates]
            if not self.candidates:
                raise Exception("No UP RAN/CORE pair matches the request (TA coverage, RAN-CORE links, link latency).")

            log.info(f"Saga-{self.intent_id}: Candidates -> "
                     + ", ".join(f"{c.ran}+{c.core}" for c in self.candidates))

            # 3. Ask the first candidate
            await self._request_feasibility()

        except Exception as e:
            log.error(f"Saga-{self.intent_id}: Preparation failed: {e}")
            await self._reject(f"Orchestration error: {str(e)}")

    async def _request_feasibility(self):
        candidate = self.candidates[self.attempt]
        self.selected_domains = {"ran": candidate.ran, "core": candidate.core}
        self.feasibility_results = {"ran": None, "core": None}
        log.info(f"Saga-{self.intent_id}: Attempt {self.attempt + 1}/{len(self.candidates)} "
                 f"-> RAN: {candidate.ran}, CORE: {candidate.core}")

        check_event = {
            "message_type": "FeasibilityCheckRequest",
            "intent_id": self.intent_id,
            "attempt": self.attempt, # Echoed back by domains, so late answers to older attempts are ignored
            "target_domains": self.selected_domains,
            "rfs_ran": self.rfs_ran,
            "rfs_core": self.rfs_core
        }
        # Note: Adapter must listen to "feasibility.check.request"
        await self.nc.publish("feasibility.check.request", json.dumps(check_event).encode())
        asyncio.create_task(self._feasibility_timeout(self.attempt))

    async def _feasibility_timeout(self, attempt):
        await asyncio.sleep(self.feasibility_timeout_s)
        if self.state != "PREPARATION" or self.attempt != attempt:
            return
        for domain_type, result in self.feasibility_results.items():
            if result is None:
                log.warning(f"Saga-{self.intent_id}: No feasibility answer from {self.selected_domains[domain_type]}.")
                self.feasibility_results[domain_type] = {
                    "status": False, "alternatives": None, "reason": "no response (timeout)"
                }
        await self.evaluate_preparation()

    async def _query_inventory_with_retry(self):
        """Retry inventory request to reduce startup race with registry/domain registration."""
        last_exception = None

        for attempt in range(1, self.inventory_retry_count + 1):
            try:
                response = await self.nc.request("domain.inventory.query", b"", timeout=2)
                payload = response.data.decode() if response and response.data else "{}"
                inventory = json.loads(payload)
                domains = inventory.get("domains", {})

                if domains:
                    return inventory

                log.warning(
                    f"Saga-{self.intent_id}: Inventory is empty (attempt {attempt}/{self.inventory_retry_count}). Retrying..."
                )
            except Exception as e:
                last_exception = e
                log.warning(
                    f"Saga-{self.intent_id}: Inventory query failed (attempt {attempt}/{self.inventory_retry_count}): {e}"
                )

            await asyncio.sleep(self.inventory_retry_delay_s)

        if last_exception is not None:
            raise Exception(f"Domain inventory query failed after retries: {last_exception}")

        raise Exception("Domain inventory is empty after retries.")

    async def on_feasibility_response(self, domain_type, attempt, is_possible, alternative_params=None, reason=None):
        """Handle response from Domain Brokers via Adapter"""
        if self.state != "PREPARATION" or attempt != self.attempt:
            log.debug(f"Saga-{self.intent_id}: Ignoring feasibility answer for attempt {attempt}.")
            return

        self.feasibility_results[domain_type] = {
            "status": is_possible,
            "alternatives": alternative_params,
            "reason": reason
        }

        # Check if both domains responded
        if all(v is not None for v in self.feasibility_results.values()):
            await self.evaluate_preparation()

    async def evaluate_preparation(self):
        ran_result = self.feasibility_results["ran"]
        core_result = self.feasibility_results["core"]
        self.attempts.append({
            "ran": self.selected_domains["ran"],
            "core": self.selected_domains["core"],
            "ran_ok": ran_result["status"],
            "core_ok": core_result["status"],
            "reasons": {k: v["reason"] for k, v in self.feasibility_results.items() if not v["status"]},
            "proposals": {k: v["alternatives"] for k, v in self.feasibility_results.items() if v["alternatives"]},
        })

        if ran_result["status"] and core_result["status"]:
            log.success(f"Saga-{self.intent_id}: Feasibility OK. Moving to COMMISSIONING.")
            self.state = "COMMISSIONING"
            await self.provision_service()
            return

        rejected_by = ", ".join(f"{self.selected_domains[k]}: {v}" for k, v in self.attempts[-1]["reasons"].items())

        # Domains that accepted this attempt hold resources for it - let them go.
        approved = {k: self.selected_domains[k] for k, v in self.feasibility_results.items() if v["status"]}
        if approved:
            await self.nc.publish("feasibility.release", json.dumps({
                "message_type": "FeasibilityRelease",
                "intent_id": self.intent_id,
                "attempt": self.attempt,
                "target_domains": approved
            }).encode())
        self.attempt += 1

        if self.attempt < len(self.candidates):
            log.warning(f"Saga-{self.intent_id}: Candidate rejected ({rejected_by}). Trying the next one.")
            await self.nc.publish("intent.status.update", json.dumps({
                "intent_id": self.intent_id,
                "message": f"Attempt {self.attempt}/{len(self.candidates)} rejected ({rejected_by}), trying next candidate.",
                "attempts": self.attempts
            }).encode())
            await self._request_feasibility()
        else:
            log.warning(f"Saga-{self.intent_id}: Feasibility FAILED for all {len(self.candidates)} candidates. Notifying Gateway.")
            await self._reject(f"Feasibility check failed for all {len(self.candidates)} candidate(s); last: {rejected_by}.")

    async def _reject(self, message):
        self.state = "REJECTED"
        await self.nc.publish("intent.status.update", json.dumps({
            "intent_id": self.intent_id,
            "current_handling_state": "REJECTED",
            "message": message,
            "attempts": self.attempts,
            # Proposals from the last attempt, e.g. the bandwidth a RAN could still offer
            "proposals": self.attempts[-1]["proposals"] if self.attempts else {}
        }).encode())

    async def provision_service(self):
        """Step 2: Technical creation (Commissioning Phase)"""
        # Generate the unique service topic for this instance
        clean_id = self.intent_id.lower().replace("-", "_")
        self.service_topic = f"unity6g.service.{clean_id}"

        dispatch_event = {
            "message_type": "ServiceOrderRequestDispatch",
            "intent_id": self.intent_id,
            "service_topic": self.service_topic, # Shared with Adapter and Domains
            "target_domains": self.selected_domains, # Use the selected IDs
            "rfs_ran": self.rfs_ran,
            "rfs_core": self.rfs_core
        }
        # Publish to the subject the Adapter is listening to
        await self.nc.publish("service.order.request.dispatch", json.dumps(dispatch_event).encode())

    async def start_lifecycle_timer(self):
        """Starts service timer"""
        log.info(f"Saga-{self.intent_id}: Service will expire in {self.duration}s")
        await asyncio.sleep(self.duration)
        
        if self.state == "OPERATION":
            log.warning(f"Saga-{self.intent_id}: Time expired. Initiating Termination.")
            await self.terminate_service()

    async def terminate_service(self):
        """Sends event ServiceTermination to DMOs."""
        self.state = "DECOMMISSIONING"
        
        termination_event = {
            "message_type": "ServiceTermination",
            "intent_id": self.intent_id,
            "service_topic": self.service_topic,
            "target_domains": self.selected_domains,
            "reason": "Lifecycle Expired"
        }
        
        await self.nc.publish("service.order.terminate", json.dumps(termination_event).encode())

    async def process_fault(self, fault_data):
        self.fault_count += 1
        violations = fault_data.get("violations", [])
        
        log.warning(f"Saga {self.intent_id}: Fault detected ({self.fault_count}/{self.MAX_FAULT_THRESHOLD}). Issues: {violations}")

        if self.fault_count < self.MAX_FAULT_THRESHOLD:
            log.info(f"Saga {self.intent_id}: Monitoring situation... (Temporary fluctuation?)")
            return

        if self.fault_count == self.MAX_FAULT_THRESHOLD:
            # TODO: recovery (e.g. re-provisioning on the next candidate) is not implemented yet.
            log.critical(f"Saga {self.intent_id}: SLS threshold exceeded consistently. Recovery needed!")

class ServiceOrchestrator:
    def __init__(self, nats_url="nats://localhost:4222", domain_selection="first_available",
                 max_candidates=3, feasibility_timeout_s=5.0, seed=None):
        self.nats_url = nats_url
        self.nc = None
        self.active_sagas = {}
        self.domain_selection = domain_selection
        self.max_candidates = max_candidates
        self.feasibility_timeout_s = feasibility_timeout_s
        self._rng = random.Random(seed)

    async def connect(self):
        self.nc = await nats.connect(self.nats_url)
        
        await self.nc.subscribe("service.order.request", cb=self.handle_new_order)       
        await self.nc.subscribe("feasibility.check.response", cb=self.handle_domain_feedback)
        await self.nc.subscribe("service.change.status", cb=self.handle_status_changed)
        await self.nc.subscribe("domain.heartbeat_lost", cb=self.handle_domain_lost)
        await self.nc.subscribe("sls.violation", cb=self.handle_fault_alerts)

        log.info("Service Orchestrator: Online.")

    async def handle_new_order(self, msg):
        data = json.loads(msg.data.decode())
        intent_id = data["intent_id"]
        saga = ServiceSaga(
            intent_id, data["rfs_ran"], data["rfs_core"], self.nc,
            select_candidates=partial(select_candidates, strategy=self.domain_selection,
                                      load=self._domain_load(), rng=self._rng),
            max_candidates=self.max_candidates,
            feasibility_timeout_s=self.feasibility_timeout_s,
        )
        self.active_sagas[intent_id] = saga
        await saga.start()
        self._drop_if_rejected(saga)

    def _domain_load(self) -> dict[str, int]:
        """Number of services each domain currently handles (sagas past the preparation phase)."""
        load = {}
        for saga in self.active_sagas.values():
            if saga.state in ("COMMISSIONING", "OPERATION", "DECOMMISSIONING"):
                for domain_id in saga.selected_domains.values():
                    load[domain_id] = load.get(domain_id, 0) + 1
        return load

    def _drop_if_rejected(self, saga):
        if saga.state == "REJECTED":
            self.active_sagas.pop(saga.intent_id, None)

    async def handle_domain_feedback(self, msg):
        """Routes feedback to the correct Saga instance"""
        data = json.loads(msg.data.decode())
        intent_id = data.get("intent_id")
        domain_type = data.get("domain_type") 
        
        if intent_id in self.active_sagas:
            saga = self.active_sagas[intent_id]
            await saga.on_feasibility_response(
                domain_type=domain_type,
                attempt=data.get("attempt", 0),
                is_possible=data["status"],
                alternative_params=data.get("proposals"),
                reason=data.get("reason")
            )
            self._drop_if_rejected(saga)

    async def handle_status_changed(self, msg):
        """Finalizes the Saga and updates Gateway"""
        data = json.loads(msg.data.decode())
        intent_id = data.get("intent_id")
        current_service_status = data.get("status")
        domain_id = data.get("domain_id")
        
        if intent_id in self.active_sagas:
            saga = self.active_sagas[intent_id]
            if current_service_status == "ACTIVE" and saga.state == "COMMISSIONING":
                role = next((k for k, v in saga.selected_domains.items() if v == domain_id), None)
                if role:
                    saga.provisioning_results[role] = True
                    log.info(f"Saga-{intent_id}: Domain {domain_id} ({role}) confirmed provisioning.")

                if all(v is True for v in saga.provisioning_results.values()):
                    log.success(f"Orchestrator: Service for {intent_id} is now ACTIVE.")
                    saga.state = "OPERATION"

                    registration_payload = {
                        "intent_id": intent_id,
                        "rfs_ran": saga.rfs_ran  
                    }
                    await self.nc.publish("monitor.service.start", json.dumps(registration_payload).encode())
                    log.info(f"Orchestrator -> SLAMonitor: Registered {intent_id}")
                    # --------------------------------------------
                    
                    final_report = {
                        "intent_id": intent_id,
                        "lifecycle_status": "ACTIVE",
                        "message": "Service is fully operational.",
                        "selected_domains": saga.selected_domains,
                        "attempts": saga.attempts
                    }
                    asyncio.create_task(saga.start_lifecycle_timer())
                    await self.nc.publish("intent.status.update", json.dumps(final_report).encode())

            elif current_service_status == "DEACTIVATED" and saga.state == "DECOMMISSIONING":
                role = next((k for k, v in saga.selected_domains.items() if v == domain_id), None)
                if role:
                    saga.termination_results[role] = True
                    log.info(f"Saga-{intent_id}: Domain {domain_id} ({role}) confirmed termination.")

                if all(v is True for v in saga.termination_results.values()):
                
                    log.warning(f"Orchestrator: Domain confirmed termination for {intent_id}.")

                    await self.nc.publish("monitor.service.stop", json.dumps({"intent_id": intent_id}).encode())
                    
                    final_report = {
                        "intent_id": intent_id,
                        "current_handling_state": "HANDLING_ENDED",
                        "message": "Service lifecycle expired and resources released."
                    }
                    del self.active_sagas[intent_id]

                    cleanup_request = {
                        "topics": [saga.service_topic]
                    }
                    await self.nc.publish("clean.topics", json.dumps(cleanup_request).encode())
                    await self.nc.publish("intent.status.update", json.dumps(final_report).encode())
  

    async def handle_domain_lost(self, msg):
        """Reacts to DomainHeartbeatLost from the Registry."""
        data = json.loads(msg.data.decode())
        lost_domain_id = data.get("domain_id")
        
        log.warning(f"Orchestrator: Received DomainHeartbeatLost for {lost_domain_id}")

        affected_intents = [
            intent_id for intent_id, saga in self.active_sagas.items() 
            if lost_domain_id in saga.selected_domains.values()
        ]

        for intent_id in affected_intents:
            await self._handle_service_recovery(intent_id, lost_domain_id)

    async def _handle_service_recovery(self, intent_id, lost_domain_id):
        log.critical(f"Saga-{intent_id}: Service interrupted by loss of domain {lost_domain_id}!")
        
        # TODO: recovery (re-provisioning on another domain) is not implemented yet.
        await self.nc.publish("monitor.service.stop", json.dumps({"intent_id": intent_id}).encode())

        # Release the service in the domain that is still alive.
        saga = self.active_sagas.get(intent_id)
        if saga is not None and saga.service_topic is not None:
            await self.nc.publish("service.order.terminate", json.dumps({
                "message_type": "ServiceTermination",
                "intent_id": intent_id,
                "service_topic": saga.service_topic,
                "target_domains": saga.selected_domains,
                "reason": f"Domain {lost_domain_id} lost"
            }).encode())
            asyncio.create_task(self._cleanup_topics_later([saga.service_topic]))

        report = {
            "intent_id": intent_id,
            "current_handling_state": "HANDLING_ENDED",  
            "message": f"Critical: Domain {lost_domain_id} lost heartbeat."
        }
        await self.nc.publish("intent.status.update", json.dumps(report).encode())
        
        if intent_id in self.active_sagas:
            del self.active_sagas[intent_id]

    async def _cleanup_topics_later(self, topics, delay_s=5.0):
        """Deletes topics after the remaining domains had time to read the last messages."""
        await asyncio.sleep(delay_s)
        await self.nc.publish("clean.topics", json.dumps({"topics": topics}).encode())

    async def handle_fault_alerts(self, msg):
        data = json.loads(msg.data.decode())
        intent_id = data.get("intent_id")
        
        if intent_id in self.active_sagas:
            saga = self.active_sagas[intent_id]
            
            await saga.process_fault(data)
        else:
            log.debug(f"Orchestrator: Received fault for unknown or closed saga {intent_id}")

    async def run(self):
        await self.connect()
        while True: await asyncio.sleep(1)