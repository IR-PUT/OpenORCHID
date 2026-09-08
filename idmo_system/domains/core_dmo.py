import random

from idmo_system.domains.base_domain import BaseDomainBroker
from loguru import logger

class CoreDomainBroker(BaseDomainBroker):
    def __init__(self, kafka_bootstrap="localhost:9092"):
        super().__init__(domain_type="core", kafka_bootstrap=kafka_bootstrap)

    async def start(self):
        core_caps = {
            "cpu": 8,
            "memory_gb": 16.0,
            "storage_gb": 100.0,
            "maxNumberofUEs": 5000
        }
        logger.info(f"CoreDomainBroker: Starting registration with capabilities...")
        await self.run(capabilities=core_caps)

    async def handle_feasibility(self, data):
        if random.random() <= 1.0:
            is_ok = True
        else:
            is_ok = False
        return is_ok, None

    async def handle_provisioning(self, data):
        return f"core_vm_{data['intent_id']}"