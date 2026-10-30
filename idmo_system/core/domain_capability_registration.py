import json
import re
import time
import asyncio
import nats
from loguru import logger as log
from idmo_system.communication.api import (
    DomainRegistrationRequest, 
    DomainRegistrationResponse, 
    DomainHealthMessage
)

class DomainCapabilityRegistration:
    def __init__(self, nats_url="nats://localhost:4222", heartbeat_interval_s=10.0, max_missed_heartbeats=3,
                 pending_timeout_s=30.0):
        self.nats_url = nats_url
        self.nc = None
        self.domain_repository = {} 
        self._heartbeat_interval_s = heartbeat_interval_s
        self._max_missed_heartbeats = max_missed_heartbeats
        
        self._timeout_threshold = self._heartbeat_interval_s * self._max_missed_heartbeats
        # A PENDING domain is still being set up (topics, consumers) - give it more time for its first heartbeat.
        self._pending_timeout_s = max(pending_timeout_s, self._timeout_threshold)

    async def connect(self):
        """Connect to NATS and subscribe to Registration and Heartbeat events."""
        self.nc = await nats.connect(self.nats_url)

        
        await self.nc.subscribe("domain.registration.request", cb=self.handle_registration)
        await self.nc.subscribe("domain.heartbeat", cb=self.handle_heartbeat)
        
        log.info("Domain & Capability Registration: Ready and monitoring heartbeats.")

        await self.nc.subscribe("domain.inventory.query", cb=self.handle_inventory_query)

        log.info("Domain & Capability Registration: Ready to answer inventory queries.")


    async def handle_inventory_query(self, msg):
        """Sends the current list of registered domains back to the requester."""
        
        inventory_data = {
            "domains": self.domain_repository
        }
        await self.nc.publish(msg.reply, json.dumps(inventory_data, default=str).encode())

    async def handle_registration(self, msg):
        """Handles DomainRegistrationRequest and initializes the heartbeat timer."""
        try:
            request = DomainRegistrationRequest.model_validate_json(msg.data)

            domain_id = self._domain_id_from_name(request.domain_name)

            if domain_id in self.domain_repository:
                log.error(f"Registration rejected: domain {domain_id} is already registered.")
                response = DomainRegistrationResponse(
                    source_module="IDMO_REGISTRY",
                    target_module=request.source_module,
                    status=False,
                    message=f"Domain name '{request.domain_name}' is already registered."
                )
                await self.nc.publish("domain.registration.ack", response.model_dump_json().encode())
                return

            # Store in Repository with a timestamp
            self.domain_repository[domain_id] = {
                "domain_id": domain_id,
                "domain_name": request.domain_name,
                "type": request.domain_type,
                "capabilities": request.capabilities.model_dump(),
                # UP only after the first heartbeat - until then the domain may not be able to receive requests yet.
                "status": "PENDING",
                "last_heartbeat": time.time(), # Initial timestamp
                "source_module": request.source_module
            }

            log.success(f"Domain {domain_id} registered. Monitoring started.")

            # Send Ack back
            response = DomainRegistrationResponse(
                source_module="IDMO_REGISTRY",
                target_module=request.source_module,
                status=True,
                domain_unique_id=domain_id,
                topic_domain_to_idmo=f"unity6g.from.{domain_id}",
                topic_idmo_to_domain=f"unity6g.to.{domain_id}",
                heartbeat_interval_s=self._heartbeat_interval_s
            )

            await self.nc.publish("domain.registration.ack", response.model_dump_json().encode())

        except Exception as e:
            log.error(f"Registration Error: {e}")

    @staticmethod
    def _domain_id_from_name(domain_name: str) -> str:
        """Domain ID is used in Kafka topic names, so only [a-zA-Z0-9_-] characters are kept."""
        return re.sub(r"[^a-zA-Z0-9_-]", "_", domain_name)

    async def handle_heartbeat(self, msg):
        """Processes DomainHealthMessage and updates 'last_heartbeat' timestamp."""
        try:
    
            data = json.loads(msg.data.decode())
        
            domain_id = data.get("source_module") 

            if domain_id in self.domain_repository:
                self.domain_repository[domain_id]["last_heartbeat"] = time.time()
                previous_status = self.domain_repository[domain_id]["status"]
                if previous_status != "UP":
                    log.info(f"Domain {domain_id} is {'back ' if previous_status == 'DOWN' else ''}UP.")
                    self.domain_repository[domain_id]["status"] = "UP"
            else:
                log.warning(f"Heartbeat from unknown domain: {domain_id}")

        except Exception as e:
            log.error(f"Heartbeat processing error: {e}")

    async def monitor_domain_health(self):
        while True:
            current_time = time.time()
           
            for domain_id in list(self.domain_repository.keys()):
                info = self.domain_repository[domain_id]
                time_since_last = current_time - info["last_heartbeat"]
                timeout = self._pending_timeout_s if info["status"] == "PENDING" else self._timeout_threshold
                
                if time_since_last > timeout:
                    log.error(f"Domain {domain_id} TIMEOUT! Deleting from registry.")
                    
                    failure_event = {"domain_id": domain_id, "reason": "Timeout - Deleted"}
                    await self.nc.publish("domain.heartbeat_lost", json.dumps(failure_event).encode())

                    cleanup_request = {
                        "topics": [f"unity6g.to.{domain_id}", f"unity6g.from.{domain_id}"]
                    }
                    await self.nc.publish("clean.topics", json.dumps(cleanup_request).encode())
                   
                    del self.domain_repository[domain_id]
                    
            await asyncio.sleep(2)


    async def run(self):
        await self.connect()
       
        await self.monitor_domain_health()

if __name__ == "__main__":
    registry = DomainCapabilityRegistration()
    asyncio.run(registry.run())