# OpenORCHID — notes for Claude

Public **demo** of an IDMO (Inter-Domain Management & Orchestration) system. The author's real project
is a separate, heavily rebuilt codebase (not in this repo). This repo should stay small and readable
as a showcase: don't over-engineer and don't import concepts from the private version unless asked.

**Ongoing work: see `PLAN.md`** (configurable YAML scenarios; one branch per stage, `refactoring-step-N`).

## Stack / run
- Python >=3.13, `uv` (`pyproject.toml` name still `idmo-first`). Deps: aiokafka, nats-py, fastapi, uvicorn, pydantic, aiohttp, loguru.
- `uv run python -m idmo_system run scenarios/<file>.yaml` (or `validate`); `run_all.py` = basic scenario.
  Scenario = YAML validated by `idmo_system/scenario.py`; `runner.py` builds everything in one process.
  `runtime.transport: mock` → `idmo_system.mocks.install()` **patches `sys.modules`** for `aiokafka`/`nats`, so
  components are imported lazily inside `ScenarioRunner.run()`; one transport per process (`mocks.reset()` between runs).
  `real` uses `docker-compose.yaml` (Kafka :9092, Kafka UI :8080, NATS :4222, Ollama :11434 `qwen2.5-coder:1.5b`).
  `runtime.llm: mock` → `MockIntentTranslator` with scenario `translations`.
- Output: `results/<scenario>/{nats,kafka,intents}.log` + `report.json` + summary on stdout (exit 1 if `expect` fails).
  `logs/` = output of the original demo.
- `idmo_system/distributed_system_run.py` — backend only (no gateway), real brokers.
- `main.py` — **stale**: imports non-existent `src.idmo`, `src.service_adapter` (leftover from an older layout).
- Tests: `uv run pytest` (~2 min, runs every `scenarios/*.yaml`; each needs an `expect` section);
  `uv run pytest -m "not scenario"` = unit tests only (~1 s). `tests/conftest.py` installs the mocks first. No linter config.

## Architecture (idmo_system/)
- Two buses: **NATS** = internal IDMO control plane; **Kafka** = IDMO <-> domain transport. All Kafka topics prefixed `unity6g.`.
- `communication/message_bus_adapter.py` (MBA): the only Kafka<->NATS bridge. Creates bootstrap topics
  `unity6g.domain.registration.{request,response}`, per-domain `unity6g.{to,from}.<domain_id>`, per-service
  `unity6g.service.<intent_id lower, '-'→'_'>`; deletes topics on `clean.topics`. Routes by `message_type`.
- `communication/api.py`: pydantic models (registration, health, RAN/CORE capabilities, **RFS: `RanRFS`/`CoreRFS`/`ResourceFacingService`**
  — the single source of RFS field names). Bus messages are plain dicts (`model_dump()`).
- `core/domain_capability_registration.py`: in-memory domain registry, heartbeat timeout → `domain.heartbeat_lost`
  + topic cleanup; answers `domain.inventory.query` (NATS request/reply). Domain ID = sanitized unique `domain_name`.
  Status `PENDING` → `UP` on first heartbeat → `DOWN`/deleted on timeout.
- `core/service_orchestrator.py`: `ServiceOrchestrator` + per-intent `ServiceSaga`
  (PREPARATION→COMMISSIONING→OPERATION→DECOMMISSIONING, or REJECTED); lifetime = `rfs_ran.duration_s` (default 60 s).
  Preparation: `core/domain_selection.py` filters + ranks (RAN, CORE) pairs, saga asks them one by one (`attempt`
  numbers, timeout) up to `max_candidates`. Load for `least_loaded` = orchestrator's own count of active sagas.
- `core/sla_monitor.py`: compares `ServiceTelemetryReport` vs RFS, publishes `intent.status.update` (STATE_COMPLIES/DEGRADES) and `sls.violation`.
- `domains/base_domain.py` `BaseDomainBroker` (Kafka only): register → heartbeats → feasibility/provisioning on
  `to.` topic (read from `earliest`), termination on service topic. `ran_dmo.py` / `core_dmo.py` take
  capabilities (api.py models) + behaviour (`domains/behavior.py`). `handle_feasibility` returns `FeasibilityResult`;
  `reserve()`/`release()` track per-intent resources (released automatically on termination).
- `gateway/intent_gateway.py`: `create_app(nats_url, translator)` → FastAPI `POST /intents` (`expression` or structured `rfs`),
  `GET /intents[/{id}]`; ISR in `app.state`, updated from `intent.status.update`.
  `gateway/translator.py`: Ollama `/api/generate` with JSON-schema `format`.
- `mocks/`: `kafka_mock.py`, `nats_mock.py` (`install()`/`reset()`), `llm_mock.py` (`MockIntentTranslator`).

NATS subjects: `service.order.request`, `domain.registration.{request,ack}`, `domain.heartbeat`, `domain.heartbeat_lost`,
`domain.inventory.query`, `feasibility.check.{request,response}`, `service.order.request.dispatch`, `service.order.terminate`,
`service.change.status`, `service.telemetry.report`, `monitor.service.{start,stop}`, `sls.violation`, `intent.status.update`, `clean.topics`.

## Known issues (after Stage 3, 2026-10)
- Resources reserved at provisioning, not at feasibility → concurrent sagas can overbook a domain.
- Recovery is TODO (`process_fault` only logs once the SLA threshold is reached; `_handle_service_recovery` ends the
  intent and releases the surviving domain). A lost domain cannot re-register under the same name while still in registry.
- `main.py` is stale (→ removed in Stage 4).
