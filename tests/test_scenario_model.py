import pytest
import yaml
from pydantic import ValidationError

from idmo_system.runner import IntentResult, check_expectation
from idmo_system.scenario import ExpectConfig, IntentConfig, Scenario

BASE = yaml.safe_load("""
name: test
domains:
  - name: ran-1
    type: ran
    capabilities: { maxNumberofUEs: 10, nROperatingBands: [n78], coverageAreaTAList: [[1, 1, 1]], positioning: true,
                    core_links: { core-1: 1.0 } }
  - name: core-1
    type: core
    capabilities: { cpu: 1, memory_gb: 1, storage_gb: 1, maxNumberofUEs: 10 }
intents:
  - rfs: { ran: { bandwidth_mhz: 10 } }
""")


def scenario(**changes):
    return Scenario.model_validate({**BASE, **changes})


def test_valid_scenario():
    s = scenario()
    assert [d.name for d in s.domains] == ["ran-1", "core-1"]
    assert s.orchestrator.domain_selection == "first_available"


@pytest.mark.parametrize("changes, message", [
    ({"runtime": {"duraton_s": 5}}, "Extra inputs are not permitted"),
    ({"domains": BASE["domains"] + [BASE["domains"][1]]}, "domain names must be unique"),
    ({"orchestrator": {"domain_selection": "best"}}, "unknown domain_selection"),
    ({"intents": [{"expression": "x", "rfs": {}}]}, "exactly one of 'expression' or 'rfs'"),
    ({"events": [{"at_s": 1, "domain": "ran-9", "action": "crash"}]}, "unknown domain 'ran-9'"),
    ({"events": [{"at_s": 1, "domain": "core-1", "action": "degrade"}]}, "only supported for RAN"),
    ({"expect": [{"intent": 3, "final_state": "REJECTED"}]}, "there are 1 intents"),
    ({"expect": [{"intent": 0, "final_state": "DONE"}]}, "final_state"),
])
def test_invalid_scenarios(changes, message):
    with pytest.raises(ValidationError, match=message):
        scenario(**changes)


def test_unknown_core_link():
    ran = BASE["domains"][0]
    bad_ran = {**ran, "capabilities": {**ran["capabilities"], "core_links": {"core-9": 1.0}}}
    with pytest.raises(ValidationError, match="unknown CORE domains"):
        scenario(domains=[bad_ran, BASE["domains"][1]])


def test_check_expectation():
    result = IntentResult(index=0, config=IntentConfig(rfs={}), intent_id="INT-1", history=[
        {"current_handling_state": "STATE_DEGRADES"},
        {"lifecycle_status": "ACTIVE", "selected_domains": {"ran": "ran-1", "core": "core-1"}, "attempts": [{}, {}]},
        {"current_handling_state": "HANDLING_ENDED", "message": "Service lifecycle expired."},
    ])
    result.handling_state = "HANDLING_ENDED"

    ok = check_expectation(ExpectConfig(intent=0, final_state="HANDLING_ENDED", ran="ran-1", attempts=2,
                                        sla_degraded=True, message_contains="expired"), result)
    assert ok.passed

    failed = check_expectation(ExpectConfig(intent=0, final_state="REJECTED", core="core-2"), result)
    assert failed.failures == ["final_state: expected 'REJECTED', got 'HANDLING_ENDED'",
                               "core: expected 'core-2', got 'core-1'"]
