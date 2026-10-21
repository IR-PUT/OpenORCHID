# OpenORCHID — notes for Claude

Public **demo** of an IDMO (Inter-Domain Management & Orchestration) system. The author's real project
is a separate, heavily rebuilt codebase (not in this repo). This repo should stay small and readable
as a showcase: don't over-engineer and don't import concepts from the private version unless asked.

**Ongoing work: see `PLAN.md`** (configurable YAML scenarios; one branch per stage, `refactoring-step-N`).

## Stack / run
- Python >=3.13, `uv` (`pyproject.toml` name still `idmo-first`). Deps: aiokafka, nats-py, fastapi, uvicorn, pydantic, aiohttp, loguru.
- `uv run python run_all.py` — the main demo. With `USE_MOCKS = True` it calls `idmo_system.mocks.install()`, which
  **patches `sys.modules`** for `aiokafka`/`nats` (must run before importing components) and `IntentTranslator.translate_to_rfs`,
  so it runs fully in-process with no Docker. Otherwise uses `docker-compose.yaml` (Kafka KRaft :9092, Kafka UI :8080,
  NATS :4222, Ollama :11434 with `qwen2.5-coder:1.5b`). Mock LLM only knows the phrases in `mocks/llm_mock.py`.
- Runs 60 s (`SHUTDOWN_AFTER_SECONDS`), submits one NL intent via HTTP (:8000) and polls status; writes
  `results/{nats_all_subjects,kafka_all_topics,user_intent}.log`. `logs/` = older copy of the same output.
- `idmo_system/distributed_system_run.py` — backend only (no gateway), real brokers.
- `main.py` — **stale**: imports non-existent `src.idmo`, `src.service_adapter` (leftover from an older layout).
- No tests, no linter config.

## Architecture (idmo_system/)
- Two buses: **NATS** = internal IDMO control plane; **Kafka** = IDMO <-> domain transport. All Kafka topics prefixed `unity6g.`.
- `communication/message_bus_adapter.py` (MBA): the only Kafka<->NATS bridge. Creates bootstrap topics
  `unity6g.domain.registration.{request,response}`, per-domain `unity6g.{to,from}.<domain_id>`, per-service
  `unity6g.service.<intent_id lower, '-'→'_'>`; deletes topics on `clean.topics`. Routes by `message_type`.
- `communication/api.py`: pydantic models (registration, health, RAN/CORE capabilities, **RFS: `RanRFS`/`CoreRFS`/`ResourceFacingService`**
  — the single source of RFS field names). Bus messages are plain dicts (`model_dump()`).
- `core/domain_capability_registration.py`: in-memory domain registry, heartbeat timeout → `domain.heartbeat_lost`
  + topic cleanup; answers `domain.inventory.query` (NATS request/reply). Domain ID = sanitized unique `domain_name`.
- `core/service_orchestrator.py`: `ServiceOrchestrator` + per-intent `ServiceSaga`
  (PREPARATION→COMMISSIONING→OPERATION→DECOMMISSIONING, or REJECTED). Picks first UP ran/core domain; lifetime = `rfs_ran.duration_s` (default 60 s).
- `core/sla_monitor.py`: compares `ServiceTelemetryReport` vs RFS, publishes `intent.status.update` (STATE_COMPLIES/DEGRADES) and `sls.violation`.
- `domains/base_domain.py` `BaseDomainBroker` (Kafka only): register → heartbeats → feasibility/provisioning on
  `to.` topic, termination on service topic. `ran_dmo.py` (bw ≤ 4000, fake telemetry every 5 s), `core_dmo.py` (always OK).
- `gateway/intent_gateway.py`: `create_app(nats_url, translator)` → FastAPI `POST /intents`, `GET /intents[/{id}]`; ISR in `app.state`, updated from `intent.status.update`.
  `gateway/translator.py`: Ollama `/api/generate` with JSON-schema `format`.
- `mocks/`: `kafka_mock.py`, `nats_mock.py`, `llm_mock.py`, each with `install()`; `mocks.install()` enables all.

NATS subjects: `service.order.request`, `domain.registration.{request,ack}`, `domain.heartbeat`, `domain.heartbeat_lost`,
`domain.inventory.query`, `feasibility.check.{request,response}`, `service.order.request.dispatch`, `service.order.terminate`,
`service.change.status`, `service.telemetry.report`, `monitor.service.{start,stop}`, `sls.violation`, `intent.status.update`, `clean.topics`.

## Known issues (after Stage 0, 2026-10)
- Only 1 RAN + 1 CORE hardcoded in `run_all.py`; domain capabilities/behaviour hardcoded in `ran_dmo.py`/`core_dmo.py` (→ Stage 1–2).
- Domain selection = first UP domain, capabilities ignored (→ Stage 2). Recovery is TODO (`process_fault`, `_handle_service_recovery`).
- Domains need a few seconds to finish registration (mock Kafka included) — intents sent earlier hit an empty inventory.
- `main.py` is stale (→ removed in Stage 4).
