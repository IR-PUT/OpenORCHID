import random

import pytest

from idmo_system.core.domain_selection import STRATEGIES, register_strategy, select_candidates


def ran(tacs, links, status="UP"):
    return {"type": "ran", "status": status,
            "capabilities": {"coverageAreaTAList": [[260, 1, t] for t in tacs], "core_links": links}}


def core(status="UP"):
    return {"type": "core", "status": status, "capabilities": {}}


DOMAINS = {
    "r1": ran([10], {"c1": 9.0, "c2": 3.0}),
    "r2": ran([10, 20], {"c1": 1.0}),
    "r3": ran([20], {}), # no core_links -> reaches every CORE, latency unknown
    "r4": ran([10], {"c1": 2.0}, status="PENDING"),
    "c1": core(),
    "c2": core(),
}


def pairs(candidates):
    return [f"{c.ran}+{c.core}" for c in candidates]


def test_first_available_keeps_registration_order_and_filters_by_ta():
    result = select_candidates(DOMAINS, {"tac": 10}, {}, "first_available")
    assert pairs(result) == ["r1+c1", "r1+c2", "r2+c1"] # r3 does not cover TA 10, r4 is not UP


def test_lowest_latency():
    result = select_candidates(DOMAINS, {"tac": 10}, {}, "lowest_latency")
    assert pairs(result) == ["r2+c1", "r1+c2", "r1+c1"]


def test_least_loaded_uses_load_of_both_domains():
    result = select_candidates(DOMAINS, {"tac": 10}, {}, "least_loaded", load={"r1": 1, "c1": 1, "c2": 1})
    assert pairs(result) == ["r2+c1", "r1+c1", "r1+c2"] # loads: r2+c1=1, r1+c1=2, r1+c2=2


def test_link_latency_limit_from_core_rfs():
    result = select_candidates(DOMAINS, {}, {"latency_ms": 5}, "first_available")
    assert pairs(result) == ["r1+c2", "r2+c1", "r3+c1", "r3+c2"] # unknown latency (r3) is not filtered out


def test_random_is_reproducible_with_seed():
    first = select_candidates(DOMAINS, {}, {}, "random", rng=random.Random(42))
    second = select_candidates(DOMAINS, {}, {}, "random", rng=random.Random(42))
    assert pairs(first) == pairs(second)


def test_unknown_strategy():
    with pytest.raises(ValueError, match="Unknown domain selection strategy"):
        select_candidates(DOMAINS, {}, {}, "best_effort")


def test_custom_strategy_can_be_registered():
    @register_strategy("reverse_for_test")
    def reverse(candidates, rng):
        return list(reversed(candidates))

    try:
        result = select_candidates(DOMAINS, {"tac": 10}, {}, "reverse_for_test")
        assert pairs(result) == ["r2+c1", "r1+c2", "r1+c1"]
    finally:
        STRATEGIES.pop("reverse_for_test")
