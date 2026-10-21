import asyncio

import pytest

from idmo_system.communication.api import UnityCoreCapabilities, UnityRanCapabilities
from idmo_system.domains.behavior import CoreBehavior, DomainEvent, RanBehavior, TelemetryConfig
from idmo_system.domains.core_dmo import CoreDomainBroker
from idmo_system.domains.ran_dmo import RANDomainBroker

RAN_CAPS = UnityRanCapabilities(maxNumberofUEs=100, nROperatingBands=["n78"], coverageAreaTAList=[(260, 1, 10)],
                                positioning=False)
CORE_CAPS = UnityCoreCapabilities(cpu=8, memory_gb=16, storage_gb=100, maxNumberofUEs=1000)


def request(intent_id, **rfs_ran):
    return {"intent_id": intent_id, "rfs_ran": rfs_ran, "rfs_core": {}}


def run(coro):
    return asyncio.run(coro)


def test_ran_feasibility_uses_remaining_capacity():
    ran = RANDomainBroker("ran", RAN_CAPS, RanBehavior(max_bandwidth_mhz=100))

    assert run(ran.handle_feasibility(request("A", bandwidth_mhz=80, tac=10))).ok
    ran.confirm("A", ran.resources_for(request("A", bandwidth_mhz=80, num_ues=60)))

    result = run(ran.handle_feasibility(request("B", bandwidth_mhz=50, num_ues=50)))
    assert not result.ok
    assert result.proposals == {"bandwidth_mhz": 20, "num_ues": 40}
    assert "50 MHz requested, 20 MHz available" in result.reason
    assert "50 UEs requested, 40 available" in result.reason

    ran.release("A")
    assert run(ran.handle_feasibility(request("B", bandwidth_mhz=50, num_ues=50))).ok


def test_ran_rejects_uncovered_ta():
    ran = RANDomainBroker("ran", RAN_CAPS)
    result = run(ran.handle_feasibility(request("A", tac=99)))
    assert not result.ok and "TA 99 not covered" in result.reason


def test_core_service_limit():
    core = CoreDomainBroker("core", CORE_CAPS, CoreBehavior(max_services=1))
    core.confirm("A", core.resources_for(request("A", num_ues=10)))
    result = run(core.handle_feasibility(request("B")))
    assert not result.ok and "service limit reached (1/1)" in result.reason
    core.release("A")
    assert run(core.handle_feasibility(request("B"))).ok


def test_holds_count_until_released_or_expired(monkeypatch):
    ran = RANDomainBroker("ran", RAN_CAPS, RanBehavior(max_bandwidth_mhz=100))
    ran.hold("A", attempt=0, amounts={"bandwidth_mhz": 60})
    assert ran.reserved("bandwidth_mhz") == 60

    ran.release_hold("A", attempt=1) # release for another attempt is ignored
    assert ran.reserved("bandwidth_mhz") == 60
    ran.release_hold("A", attempt=0)
    assert ran.reserved("bandwidth_mhz") == 0

    ran.hold("B", attempt=0, amounts={"bandwidth_mhz": 30})
    ran.confirm("B", {"bandwidth_mhz": 30})
    ran.release_hold("B", attempt=0) # a provisioned service is released only by termination
    assert ran.reserved("bandwidth_mhz") == 30

    ran.hold("C", attempt=0, amounts={"bandwidth_mhz": 10})
    monkeypatch.setattr(ran, "HOLD_TTL_S", -1) # every unconfirmed hold is now expired
    assert ran.reserved("bandwidth_mhz") == 30


def ran_with_telemetry(**telemetry):
    return RANDomainBroker("ran", RAN_CAPS, RanBehavior(telemetry=TelemetryConfig(**telemetry)), seed=1)


def test_constant_telemetry():
    ran = ran_with_telemetry(throughput_mbps=100, latency_ms=10)
    assert ran.compute_metrics(0) == ran.compute_metrics(5) == {"throughput_mbps": 100, "latency_ms": 10}


def test_degrading_telemetry():
    ran = ran_with_telemetry(profile="degrading", degradation_per_report=0.1, throughput_mbps=100, latency_ms=10)
    assert ran.compute_metrics(0) == {"throughput_mbps": 100, "latency_ms": 10}
    assert ran.compute_metrics(2) == {"throughput_mbps": 81, "latency_ms": pytest.approx(12.35, abs=0.01)}


def test_random_telemetry_stays_within_jitter_and_is_seeded():
    values = [ran_with_telemetry(profile="random", jitter=0.1).compute_metrics(0) for _ in range(2)]
    assert values[0] == values[1]
    ran = ran_with_telemetry(profile="random", jitter=0.1, throughput_mbps=100)
    assert all(90 <= ran.compute_metrics(i)["throughput_mbps"] <= 110 for i in range(50))


def test_degrade_and_restore_events():
    ran = ran_with_telemetry(throughput_mbps=100, latency_ms=10)
    run(ran.apply_event(DomainEvent(at_s=0, domain="ran", action="degrade", throughput_factor=0.5, latency_factor=3)))
    assert ran.compute_metrics(0) == {"throughput_mbps": 50, "latency_ms": 30}
    run(ran.apply_event(DomainEvent(at_s=0, domain="ran", action="restore")))
    assert ran.compute_metrics(0) == {"throughput_mbps": 100, "latency_ms": 10}


def test_core_does_not_support_degrade():
    core = CoreDomainBroker("core", CORE_CAPS)
    with pytest.raises(ValueError):
        run(core.apply_event(DomainEvent(at_s=0, domain="core", action="degrade")))
