"""Example of a user-defined domain broker, plugged into a scenario without changing idmo_system:

    domains:
      - name: ran-hospital
        type: ran
        class: examples.priority_ran:PriorityRanBroker
        params: { protected_mhz: 50 }
        capabilities: { ... }

A custom broker subclasses RANDomainBroker (or CoreDomainBroker) and overrides the domain logic:
handle_feasibility() / handle_provisioning() / compute_metrics() / apply_event().
Entries from `params` are passed to the constructor as keyword arguments.
"""

from idmo_system.domains.base_domain import FeasibilityResult
from idmo_system.domains.ran_dmo import RANDomainBroker


class PriorityRanBroker(RANDomainBroker):
    """RAN that keeps `protected_mhz` of its bandwidth free for priority-1 (critical) services."""

    def __init__(self, *args, protected_mhz: float = 0.0, **kwargs):
        super().__init__(*args, **kwargs)
        self.protected_mhz = protected_mhz

    async def handle_feasibility(self, data):
        result = await super().handle_feasibility(data)
        priority = (data.get("rfs_core") or {}).get("priority")
        if not result.ok or priority == 1:
            return result

        free_bw = self.behavior.max_bandwidth_mhz - self.reserved("bandwidth_mhz")
        available = max(0.0, free_bw - self.protected_mhz)
        requested_bw = (data.get("rfs_ran") or {}).get("bandwidth_mhz") or 0
        if requested_bw > available:
            return FeasibilityResult(
                False,
                proposals={"bandwidth_mhz": available},
                reason=f"{requested_bw:g} MHz requested, {available:g} MHz available "
                       f"({self.protected_mhz:g} MHz kept for priority-1 services)",
            )
        return result
