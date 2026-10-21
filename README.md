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
| `idmo_system/mock_*.py` | In-process mocks for messaging and LLM, for local demos/tests |

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
 #   at_s  intent_id     final state        SLA       last message
 0    0.0  INT-E9961BF2  HANDLING_ENDED     -         Service lifecycle expired and resources released.
 1    2.0  INT-AECFC220  REJECTED           -         Feasibility check failed in at least one domain.
 2    4.0  INT-1139EDE9  HANDLING_ENDED     degraded  Service lifecycle expired and resources released.
 3    6.0  -             SUBMISSION_FAILED  -         HTTP 422: Translation Error: Translator returned no data for the given expression.
```

A minimal scenario:

```yaml
name: my-scenario
runtime: { duration_s: 60, transport: mock, llm: mock }   # transport: real / llm: ollama -> docker compose up -d
domains:
  - name: ran-1
    type: ran
    capabilities: { maxNumberofUEs: 100, nROperatingBands: [n78], coverageAreaTAList: [[260, 1, 100]], positioning: true }
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

Runtime traces (all NATS and Kafka messages, intent updates) are written to `results/<scenario name>/`. API status can be inspected via the gateway endpoints (`GET /intents`, `GET /intents/{intent_id}`).