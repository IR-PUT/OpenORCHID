# OpenORCHID — refactoring plan: configurable scenarios

## Motivation
Reviewer feedback: the project is "a script to run" rather than code that can be adapted. Topology
(1 RAN + 1 CORE), capabilities, intents and domain behaviour are hardcoded in `run_all.py`,
`ran_dmo.py`, `core_dmo.py`.

**Goal:** the scenario is *data, not code*. A YAML file describes domains, intents and events; a runner
builds the system, runs it and produces a report. The same API is usable from Python.

## Decisions
- Scenario format: **YAML** (`pyyaml`), validated with pydantic models.
- **No distributed mode** for now (everything in one process; `transport: mock | real`).
- Orchestrator **retries with the next candidate** domain when feasibility is rejected (a showcase feature).
- Design is our own — not meant to mirror the author's private version.

## Target scenario (sketch)
```yaml
name: multi-domain-urban
runtime: { duration_s: 120, transport: mock, llm: mock, seed: 42 }
registry: { heartbeat_interval_s: 5, max_missed_heartbeats: 3 }
orchestrator: { domain_selection: lowest_latency, max_candidates: 3 }
domains:
  - name: core-1
    type: core
    capabilities: { cpu: 16, memory_gb: 64, storage_gb: 500, maxNumberofUEs: 5000 }
    behavior: { max_services: 4 }
  - name: ran-urban-1
    type: ran
    capabilities:
      maxNumberofUEs: 1000
      nROperatingBands: [n78, n258]
      coverageAreaTAList: [[260, 6, 1000], [260, 6, 2000]]
      positioning: true
      core_links: { core-1: 5.0 }            # RAN->CORE latency [ms]
    behavior:
      max_bandwidth_mhz: 400                  # capacity, reserved/released per service
      telemetry: { interval_s: 5, profile: degrading, throughput_mbps: 90, latency_ms: 10 }
intents:
  - at_s: 5
    expression: "Slice for 200 UEs in TA 1000, 100 MHz, latency under 20 ms, for 30 s"
  - at_s: 10
    rfs:                                       # bypasses the translator
      ran:  { bandwidth_mhz: 300, latency_ms: 20, tac: 1000, duration_s: 40 }
      core: { priority: 1, latency_ms: 15 }
events:
  - { at_s: 50, domain: ran-urban-1, action: stop_heartbeat }
expect:
  - { intent: 0, final: HANDLING_ENDED }
  - { intent: 1, final: REJECTED }
```
CLI: `python -m idmo_system run|validate scenarios/<file>.yaml` → report (per-intent timeline, selected
domains, rejection reasons, SLA violations); non-zero exit code when an `expect` fails.

## Stages

### Stage 0 — cleanup (prerequisite) · branch `refactoring-step-0`
- [x] One pydantic RFS model (`RanRFS`, `CoreRFS`, `ResourceFacingService`) shared by translator, mock LLM,
      domains and SLA monitor; adds `tac`, `num_ues`, `throughput_mbps`, `duration_s`.
- [x] NATS/Kafka/LLM addresses injected via constructors (no hardcoded `localhost` inside components).
- [x] Domain IDs derived from (unique) domain name; duplicate registration rejected.
- [x] Bug fixes: service-topic cleanup name; REJECTED reaching the gateway; rejected/lost sagas removed;
      SLA monitoring stopped on domain loss.
- [x] Gateway: single NATS connection, lifespan instead of `on_event`, app factory, tz-aware timestamps.
- [x] Mocks enabled explicitly (`idmo_system.mocks.install()`), not as an import side effect; unused `mock_broker.py` removed.
- [x] Demo intent has a short duration so the full lifecycle (incl. decommissioning) is visible in 60 s.

### Stage 1 — scenario model + runner · branch `refactoring-step-1`
- [x] `idmo_system/scenario.py`: pydantic models for the whole file (`extra="forbid"` → typos reported with path),
      cross-checks (unique domain names, `core_links` refer to existing CORE domains).
- [x] `idmo_system/runner.py`: `ScenarioRunner(scenario).run()` → `ScenarioResult` — builds components, starts N domains,
      waits for registration (t=0), submits intents at `at_s` via the gateway HTTP API, tracks `intent.status.update`,
      stops at `duration_s` or when all intents are terminal; logs to `results/<scenario>/{nats,kafka,intents}.log`.
- [x] CLI `python -m idmo_system run|validate <file>`; `run_all.py` is a thin wrapper over `scenarios/basic.yaml`.
- [x] `RANDomainBroker` / `CoreDomainBroker` take capabilities + behaviour (`domains/behavior.py`).
- [x] Gateway accepts structured `rfs` (bypasses the LLM); mock LLM translations can be defined in the scenario.
- [x] Scenarios: `basic.yaml`, `multi_domain.yaml`.
- Events (fault injection) moved to Stage 3, where the behaviours they trigger are implemented.

### Stage 2 — capability-driven decisions · branch `refactoring-step-2`
- [x] Domain-side feasibility from capabilities + current load (reserve on provisioning, release on termination);
      RAN: TA coverage, `max_bandwidth_mhz`, `maxNumberofUEs`; CORE: `max_services`, `maxNumberofUEs`.
      Rejection returns `reason` + `proposals` (`FeasibilityResult` in `base_domain.py`).
- [x] Orchestrator: pluggable selection strategy (`core/domain_selection.py`, `@register_strategy`):
      `first_available`, `least_loaded`, `lowest_latency`, `random` (seeded). Candidates filtered by status UP,
      TA coverage, RAN↔CORE link and link latency (`rfs_core.latency_ms`), then ranked. Bands skipped (no band in RFS).
- [x] **Retry with next candidate** pair on feasibility rejection (bounded by `max_candidates`); `attempt` number
      echoed by domains (stale answers ignored), feasibility timeout counts as rejection; attempts in status updates + summary.
- [x] Registry: domain is `PENDING` until its first heartbeat (fixes requests sent to not-yet-listening domains);
      capabilities served as JSON (were a Python repr string).
- [x] `multi_domain.yaml` rewritten so each mechanism shows in the result.
- ~~Known limitation: resources reserved at provisioning~~ → fixed in Stage 4 (holds at feasibility).

### Stage 3 — behaviours and results · branch `refactoring-step-3`
- [x] Telemetry profiles (`constant`, `random` with `jitter` + `runtime.seed`, `degrading`) in `TelemetryConfig`.
- [x] Fault injection: scenario `events` (`stop_heartbeat`, `crash`, `degrade`, `restore`) → `broker.apply_event()`.
      Domain brokers track their tasks (`_spawn`/`stop()`); the runner stops all domains at the end.
- [x] On domain loss the orchestrator terminates the service in the surviving domain (releases its resources).
- [x] Report: `results/<scenario>/report.json` + console summary (events, expectations, PASSED/FAILED).
- [x] `expect` assertions (`final_state`, `ran`, `core`, `attempts`, `sla_degraded`, `message_contains`);
      CLI exits 1 on failure.
- [x] pytest: unit tests (selection, feasibility, telemetry, validation) + every `scenarios/*.yaml` in mock mode
      (`-m "not scenario"` for the fast part).
- [x] Scenarios `domain_failure.yaml`, `sla_degradation.yaml`.
- [x] Fixes found by the new scenarios: PENDING domains get their own (longer) timeout; the MBA handles
      registration ACKs concurrently (startup of N domains ~2 s instead of ~2 s per domain).

### Stage 4 — extensibility and docs · branch `refactoring-step-4`
- [x] `class: package.module:ClassName` + `params` in domain config for user-provided brokers
      (`runner.load_broker_class`, imported after the mocks); example `examples/priority_ran.py` + `custom_domain.yaml`.
- [x] Scenario `capacity_exhaustion.yaml`.
- [x] Resources **held at feasibility** (keyed by intent + attempt), confirmed at provisioning, released by
      `FeasibilityRelease` when the saga moves on, expiring after `HOLD_TTL_S` (30 s) — fixes overbooking found by the
      new scenarios. Domain API: `resources_for()` instead of reserving in `handle_provisioning()`.
- [x] README rewritten around scenarios; stale `main.py` and `logs/` removed; package renamed `openorchid`.

### Stage 5 — real Kafka · branch `refactoring-step-5`
- [x] `runtime.transport` split into `runtime.kafka` / `runtime.nats` (mock | real); CLI overrides `--kafka`, `--nats`, `--llm`.
- [x] All scenarios pass with real Kafka (docker `broker`) + mocked NATS/LLM. Fixes found on the way:
  - `localhost` → `127.0.0.1` (defaults + Kafka advertised listener): IPv6 `::1` first cost ~21 s per connection on Windows.
  - Consumers read from `earliest` (MBA pattern consumer, service listeners): a real group join takes seconds and
    messages produced meanwhile were skipped (registrations never arrived).
  - Autocommit off everywhere; the MBA commits manually per message. With autocommit, deleting a consumed topic
    (service cleanup, lost domain) made aiokafka retry the commit forever → consumer stalled, `stop()` hung.
  - Kafka clients closed on shutdown (`BaseDomainBroker.stop()`, `MessageBusAdapter.stop()`).
- [x] README: real-infrastructure instructions and a Known Limitations section. Recovery deliberately not implemented.

### Stage 6 — real NATS · branch `refactoring-step-6`
- [x] All scenarios pass with real NATS (Kafka mocked) and with real NATS + real Kafka — no code changes were needed
      for NATS semantics (callbacks, request/reply via `_INBOX`, `>` monitor) beyond what the mock already modelled.
- [x] Runner closes the components' NATS connections at the end of a run (several real-NATS runs in one process
      no longer leave connections open).
- [x] `tests/test_real_infra.py` (`-m real_infra`): every scenario in a subprocess against docker-compose services,
      enabled by `OPENORCHID_REAL=kafka,nats`, skipped otherwise.

## Possible next steps
- Recovery: re-provision on the next candidate after domain loss / persistent SLA violation (reuse the retry loop).
- Verify Ollama (`--llm ollama`).
- Strategy plugins from the scenario file (today: register in Python before loading the scenario).
- A domain re-registering after it was declared lost; distributed mode (domains as separate processes).
