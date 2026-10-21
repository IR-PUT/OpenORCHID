"""Choosing RAN + CORE domain pairs for a service.

Selection has two steps:
1. Filtering (static, from registered capabilities): domain UP, RAN covers the requested TA,
   RAN has a link to the CORE, link latency within rfs_core.latency_ms.
2. Ranking with a strategy. The saga then asks the ranked pairs for feasibility one by one
   (the domains check their current load), until one pair accepts.

Custom strategies can be added with @register_strategy("name").
"""

import random
from dataclasses import dataclass
from typing import Callable


@dataclass
class Candidate:
    ran: str
    core: str
    link_latency_ms: float | None # None = RAN does not declare core_links (any CORE reachable, latency unknown)
    load: int = 0 # Services currently handled by both domains (from the orchestrator's point of view)


# A strategy orders the filtered candidates; rng is seeded by the orchestrator for reproducibility.
Strategy = Callable[[list[Candidate], random.Random], list[Candidate]]

STRATEGIES: dict[str, Strategy] = {}


def register_strategy(name: str):
    def decorator(func: Strategy) -> Strategy:
        STRATEGIES[name] = func
        return func
    return decorator


@register_strategy("first_available")
def first_available(candidates, rng):
    """Registration order (the original behaviour)."""
    return list(candidates)


@register_strategy("least_loaded")
def least_loaded(candidates, rng):
    return sorted(candidates, key=lambda c: c.load)


@register_strategy("lowest_latency")
def lowest_latency(candidates, rng):
    return sorted(candidates, key=lambda c: float("inf") if c.link_latency_ms is None else c.link_latency_ms)


@register_strategy("random")
def random_order(candidates, rng):
    shuffled = list(candidates)
    rng.shuffle(shuffled)
    return shuffled


def filter_candidates(domains: dict, rfs_ran: dict, rfs_core: dict, load: dict[str, int]) -> list[Candidate]:
    """Builds all RAN x CORE pairs allowed by the registered capabilities, in registration order."""
    rans = {i: d for i, d in domains.items() if d["type"] == "ran" and d["status"] == "UP"}
    cores = {i: d for i, d in domains.items() if d["type"] == "core" and d["status"] == "UP"}

    tac = rfs_ran.get("tac")
    max_link_latency = rfs_core.get("latency_ms")

    candidates = []
    for ran_id, ran in rans.items():
        caps = ran["capabilities"]
        if tac is not None and tac not in [ta[2] for ta in caps["coverageAreaTAList"]]:
            continue

        links = caps.get("core_links") or {}
        for core_id in cores:
            if links and core_id not in links:
                continue
            latency = links.get(core_id)
            if max_link_latency is not None and latency is not None and latency > max_link_latency:
                continue
            candidates.append(Candidate(ran_id, core_id, latency, load.get(ran_id, 0) + load.get(core_id, 0)))
    return candidates


def select_candidates(domains: dict, rfs_ran: dict, rfs_core: dict, strategy: str,
                      load: dict[str, int] | None = None, rng: random.Random | None = None) -> list[Candidate]:
    if strategy not in STRATEGIES:
        raise ValueError(f"Unknown domain selection strategy '{strategy}', available: {sorted(STRATEGIES)}")
    candidates = filter_candidates(domains, rfs_ran, rfs_core, load or {})
    return STRATEGIES[strategy](candidates, rng or random.Random())
