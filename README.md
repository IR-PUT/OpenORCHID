# OpenORCHID

<h3 align="center">
OpenORCHID - Open version of ORCHestration for Inter-Domain management
</h3>
<p align="center">
    <img src="orchid-logo.svg" alt="ORCHID Logo" width="200">
</p>


OpenORCHID is an event-driven orchestration system for managing multi-domain network services (e.g., RAN and CORE). It combines service lifecycle control via the **Saga pattern**, a **dual-bus (NATS + Kafka) integration layer**, and **LLM-based translation** of natural-language intents into structured service requests.

## Key Ideas

- **Hybrid event architecture** — NATS handles orchestration/control-plane events, while Kafka handles domain/service transport and dynamic channelization.
- **Saga-based lifecycle** — every intent is driven through Preparation → Commissioning → Operation → Decommissioning phases, correlating asynchronous responses across domains.
- **Dynamic infrastructure** — domain registration and service provisioning automatically create the Kafka topics they need, and lifecycle completion triggers cleanup.
- **Intent-driven ingress** — a FastAPI gateway accepts natural-language intents and uses an LLM-backed translator to produce a strict, validated schema (RFS) for RAN and CORE domains.
- **Built-in observability** — an SLA monitor closes the loop by comparing live telemetry against target constraints and raising compliance/fault signals.

## Architecture at a Glance

| Layer | Responsibility |
|---|---|
| `idmo_system/core/` | Service orchestration (Saga), domain registry, SLA monitoring |
| `idmo_system/communication/` | NATS↔Kafka bridge and shared message contracts |
| `idmo_system/domains/` | Domain broker base + RAN/CORE-specific behavior |
| `idmo_system/gateway/` | HTTP intent ingress and NL→RFS translation |
| `idmo_system/mocks/` | In-process mocks for Kafka, NATS and the LLM, for local demos/tests |
| `idmo_system/scenario.py`, `runner.py` | Scenario files (YAML) and the runner that builds and drives the system |

## Getting Started

The system is driven by **scenario files** (YAML) describing the domains (capabilities and behaviour),
the intents to submit and the runtime settings. See [`scenarios/`](scenarios/) for examples.

```bash
# Check a scenario file
uv run python -m idmo_system validate scenarios/multi_domain.yaml

# Run it - by default with in-process mocks of Kafka, NATS and the LLM, no infrastructure needed
uv run python -m idmo_system run scenarios/multi_domain.yaml

# The original single-intent demo (same as: run scenarios/basic.yaml)
uv run python run_all.py
```

At the end the runner prints a summary of every intent, e.g.:

```
 #   at_s  intent_id     final state        domains (RAN+CORE)          tries  SLA       last message
 0    0.0  INT-741735FE  HANDLING_ENDED     ran-urban+core-edge             1  -         Service lifecycle expired and resources released.
 1    2.0  INT-651EA11F  REJECTED           -                               4  -         Feasibility check failed for all 4 candidate(s); last: ran-suburban: 1000 MHz requested, 200 MHz available.
 2    4.0  INT-B309317B  HANDLING_ENDED     ran-urban+core-edge             1  degraded  Service lifecycle expired and resources released.
 3    8.0  INT-35E514D4  HANDLING_ENDED     ran-suburban+core-central       4  -         Service lifecycle expired and resources released.
 4   10.0  INT-9441C816  REJECTED           -                               0  -         Orchestration error: No UP RAN/CORE pair matches the request (TA coverage, RAN-CORE links, link latency).
 5   12.0  -             SUBMISSION_FAILED  -                               0  -         HTTP 422: Translation Error: Translator returned no data for the given expression.
```

### How domains are chosen

1. **Filtering** (orchestrator, from registered capabilities): the domain is UP, the RAN covers the requested
   TA (`rfs.ran.tac`), the RAN has a link to the CORE (`core_links`) and the link latency is within `rfs.core.latency_ms`.
2. **Ranking** with the strategy set in `orchestrator.domain_selection`: `first_available`, `least_loaded`,
   `lowest_latency` or `random`. Own strategies can be added with `@register_strategy("name")` in
   `idmo_system/core/domain_selection.py`.
3. **Feasibility with retries**: the saga asks the best (RAN, CORE) pair; each domain checks its *current* load
   (RAN: bandwidth, UEs, coverage; CORE: number of services, UEs). If either rejects, the next pair is tried,
   up to `orchestrator.max_candidates`. A rejected intent reports the reasons and what the domains could offer instead.

A minimal scenario:

```yaml
name: my-scenario
runtime: { duration_s: 60, transport: mock, llm: mock }   # transport: real / llm: ollama -> docker compose up -d
domains:
  - name: ran-1
    type: ran
    capabilities: { maxNumberofUEs: 100, nROperatingBands: [n78], coverageAreaTAList: [[260, 1, 100]], positioning: true,
                    core_links: { core-1: 5.0 } }
    behavior: { max_bandwidth_mhz: 400, telemetry: { interval_s: 5, throughput_mbps: 80, latency_ms: 10 } }
  - name: core-1
    type: core
    capabilities: { cpu: 8, memory_gb: 16, storage_gb: 100, maxNumberofUEs: 5000 }
intents:
  - at_s: 0
    rfs: { ran: { bandwidth_mhz: 100, latency_ms: 20, duration_s: 20 }, core: { priority: 1 } }
```

Scenarios can also be run from Python:

```python
import asyncio

from idmo_system.scenario import Scenario
from idmo_system.runner import ScenarioRunner

result = asyncio.run(ScenarioRunner(Scenario.from_yaml("scenarios/basic.yaml")).run())
print(result.summary())
```

### Domain behaviour, events and expectations

Simulated domains can be given different **telemetry profiles** (`constant`, `random`, `degrading`), and a scenario can
inject **events** at given times: `crash` (the domain stops completely), `stop_heartbeat` (the registry declares it lost),
`degrade` / `restore` (RAN telemetry worsens temporarily). An `expect` section turns a scenario into a test:

```yaml
events:
  - { at_s: 5, domain: ran-a, action: crash }
  - { at_s: 5, domain: ran-b, action: degrade, throughput_factor: 0.5, latency_factor: 3.0 }
expect:
  - { intent: 0, final_state: HANDLING_ENDED, ran: ran-a, message_contains: "lost heartbeat" }
  - { intent: 1, final_state: REJECTED, attempts: 4 }
  - { intent: 2, sla_degraded: true }
```

`python -m idmo_system run` exits with code 1 when an expectation fails. Example scenarios:

| Scenario | Shows |
|---|---|
| `basic` | One RAN, one CORE, one intent through the whole lifecycle |
| `multi_domain` | Domain selection (link latency, TA coverage), capacity limits, retries with the next candidate |
| `domain_failure` | A crashed RAN and a RAN without heartbeats; resources released for later intents |
| `sla_degradation` | Degrading, randomly fluctuating and temporarily degraded telemetry vs. the SLA |

### Tests

```bash
uv run pytest                     # unit tests + every scenario in scenarios/ (~2 min)
uv run pytest -m "not scenario"   # unit tests only (~1 s)
```

Runtime traces (all NATS and Kafka messages, intent updates) and a `report.json` with the full result are written to `results/<scenario name>/`.API status can be inspected via the gateway endpoints (`GET /intents`, `GET /intents/{intent_id}`).