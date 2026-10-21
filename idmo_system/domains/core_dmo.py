from idmo_system.communication.api import UnityCoreCapabilities
from idmo_system.domains.base_domain import BaseDomainBroker
from idmo_system.domains.behavior import CoreBehavior

DEFAULT_CORE_CAPABILITIES = UnityCoreCapabilities(cpu=8, memory_gb=16.0, storage_gb=100.0, maxNumberofUEs=5000)


class CoreDomainBroker(BaseDomainBroker):
    def __init__(self, domain_name=None, capabilities: UnityCoreCapabilities = DEFAULT_CORE_CAPABILITIES,
                 behavior: CoreBehavior | None = None, kafka_bootstrap="localhost:9092"):
        super().__init__(domain_type="core", capabilities=capabilities.model_dump(),
                         domain_name=domain_name, kafka_bootstrap=kafka_bootstrap)
        self.behavior = behavior or CoreBehavior()

    async def handle_feasibility(self, data):
        return True, None

    async def handle_provisioning(self, data):
        return f"core_vm_{data['intent_id']}"
