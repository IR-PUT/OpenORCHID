import asyncio

import pytest

from examples.priority_ran import PriorityRanBroker
from idmo_system.communication.api import UnityRanCapabilities
from idmo_system.domains.behavior import RanBehavior
from idmo_system.domains.core_dmo import CoreDomainBroker
from idmo_system.domains.ran_dmo import RANDomainBroker
from idmo_system.runner import load_broker_class

RAN_CAPS = UnityRanCapabilities(maxNumberofUEs=100, nROperatingBands=["n78"], coverageAreaTAList=[(260, 1, 10)],
                                positioning=False)


def request(bandwidth_mhz, priority):
    return {"intent_id": "X", "rfs_ran": {"bandwidth_mhz": bandwidth_mhz}, "rfs_core": {"priority": priority}}


def test_priority_ran_keeps_protected_bandwidth():
    ran = PriorityRanBroker("ran", RAN_CAPS, RanBehavior(max_bandwidth_mhz=100), protected_mhz=50)

    assert asyncio.run(ran.handle_feasibility(request(50, priority=3))).ok
    result = asyncio.run(ran.handle_feasibility(request(60, priority=3)))
    assert not result.ok and result.proposals == {"bandwidth_mhz": 50}
    assert asyncio.run(ran.handle_feasibility(request(100, priority=1))).ok


@pytest.mark.parametrize("path", ["examples.priority_ran:PriorityRanBroker", "examples.priority_ran.PriorityRanBroker"])
def test_load_broker_class(path):
    assert load_broker_class(path, RANDomainBroker) is PriorityRanBroker


@pytest.mark.parametrize("path, base, message", [
    ("examples.missing:Broker", RANDomainBroker, "Cannot load"),
    ("examples.priority_ran:Missing", RANDomainBroker, "Cannot load"),
    ("examples.priority_ran:PriorityRanBroker", CoreDomainBroker, "must be a subclass of CoreDomainBroker"),
])
def test_load_broker_class_errors(path, base, message):
    with pytest.raises(ValueError, match=message):
        load_broker_class(path, base)
