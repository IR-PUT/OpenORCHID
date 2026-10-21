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

```bash
# Run the full demo (startup, simulation, event monitoring).
# By default it uses in-process mocks of Kafka, NATS and the LLM - no infrastructure needed.
uv run python run_all.py

# To run against real infrastructure (Kafka, NATS, Ollama, Kafka UI),
# set USE_MOCKS = False in run_all.py and bring up the services first:
docker compose up -d
```

Runtime traces and monitoring output are written to `logs/` and `results/`. API status can be inspected via the gateway endpoints (`GET /intents`, `GET /intents/{intent_id}`).