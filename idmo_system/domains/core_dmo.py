from idmo_system.communication.api import UnityCoreCapabilities
from idmo_system.domains.base_domain import BaseDomainBroker, FeasibilityResult
from idmo_system.domains.behavior import CoreBehavior

DEFAULT_CORE_CAPABILITIES = UnityCoreCapabilities(cpu=8, memory_gb=16.0, storage_gb=100.0, maxNumberofUEs=5000)


class CoreDomainBroker(BaseDomainBroker):
    def __init__(self, domain_name=None, capabilities: UnityCoreCapabilities = DEFAULT_CORE_CAPABILITIES,
                 behavior: CoreBehavior | None = None, kafka_bootstrap="localhost:9092"):
        super().__init__(domain_type="core", capabilities=capabilities.model_dump(),
                         domain_name=domain_name, kafka_bootstrap=kafka_bootstrap)
        self.behavior = behavior or CoreBehavior()

    async def handle_feasibility(self, data):
        reasons = []
        max_services = self.behavior.max_services
        if max_services is not None and self.active_services >= max_services:
            reasons.append(f"service limit reached ({self.active_services}/{max_services})")

        # The CORE serves the UEs of the whole slice, requested on the RAN side.
        free_ues = self.capabilities["maxNumberofUEs"] - self.reserved("num_ues")
        requested_ues = (data.get("rfs_ran") or {}).get("num_ues") or 0
        if requested_ues > free_ues:
            reasons.append(f"{requested_ues} UEs requested, {free_ues:g} available")

        if reasons:
            return FeasibilityResult(False, proposals={"num_ues": free_ues}, reason="; ".join(reasons))
        return FeasibilityResult(True)

    async def handle_provisioning(self, data):
        requested_ues = (data.get("rfs_ran") or {}).get("num_ues") or 0
        self.reserve(data["intent_id"], num_ues=requested_ues)
        return f"core_vm_{data['intent_id']}"
