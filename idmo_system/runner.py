"""Runs a Scenario: builds all IDMO components and domains, submits intents and collects their outcome.

    scenario = Scenario.from_yaml("scenarios/basic.yaml")
    result = asyncio.run(ScenarioRunner(scenario).run())
    print(result.summary())

Components are imported lazily inside run(), after the Kafka/NATS mocks are installed (transport: mock).
One process can use only one transport, because the mocks replace aiokafka/nats in sys.modules.
"""

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import aiohttp
from loguru import logger as log

from idmo_system.scenario import IntentConfig, Scenario

TERMINAL_HANDLING_STATES = ("HANDLING_ENDED", "REJECTED")

_active_transport = None


def _prepare_transport(transport: str):
    global _active_transport
    if _active_transport is not None and _active_transport != transport:
        raise RuntimeError(f"Transport '{_active_transport}' already in use in this process, cannot switch to '{transport}'.")
    if transport == "mock":
        from idmo_system import mocks
        if _active_transport is None:
            mocks.install(llm=False)
        mocks.reset()
    _active_transport = transport


@dataclass
class IntentResult:
    index: int
    config: IntentConfig
    intent_id: str | None = None
    submitted_at_s: float | None = None
    error: str | None = None # Submission error (e.g. translation failed)
    lifecycle_status: str | None = None
    handling_state: str | None = None
    history: list[dict] = field(default_factory=list) # Status updates with "t_s" relative to t=0

    @property
    def is_terminal(self) -> bool:
        return self.error is not None or self.handling_state in TERMINAL_HANDLING_STATES

    def _latest(self, key: str):
        return next((h[key] for h in reversed(self.history) if key in h), None)

    @property
    def selected_domains(self) -> dict | None:
        """RAN and CORE domain the service was provisioned on."""
        return self._latest("selected_domains")

    @property
    def attempts(self) -> list[dict]:
        """Feasibility attempts made by the orchestrator (one per tried RAN/CORE pair)."""
        return self._latest("attempts") or []

    @property
    def sla_degraded(self) -> bool:
        """True if the SLA monitor reported degradation at any point of the service lifetime."""
        return any(h.get("current_handling_state") == "STATE_DEGRADES" for h in self.history)

    @property
    def final_state(self) -> str:
        if self.error is not None:
            return "SUBMISSION_FAILED"
        if self.intent_id is None:
            return "NOT_SUBMITTED"
        return self.handling_state or self.lifecycle_status or "UNKNOWN"


@dataclass
class ScenarioResult:
    scenario: str
    registered_domains: list[str]
    missing_domains: list[str]
    intents: list[IntentResult]
    elapsed_s: float = 0.0

    def summary(self) -> str:
        lines = [
            f"Scenario '{self.scenario}' finished after {self.elapsed_s:.1f}s",
            f"Registered domains: {', '.join(self.registered_domains) or '-'}",
        ]
        if self.missing_domains:
            lines.append(f"NOT registered: {', '.join(self.missing_domains)}")
        lines.append(f"{'#':>2}  {'at_s':>5}  {'intent_id':<12}  {'final state':<17}  {'domains (RAN+CORE)':<26}  "
                     f"{'tries':>5}  {'SLA':<8}  last message")
        for r in self.intents:
            last_message = r.error or r._latest("message") or ""
            sla = "degraded" if r.sla_degraded else "-"
            domains = f"{r.selected_domains['ran']}+{r.selected_domains['core']}" if r.selected_domains else "-"
            lines.append(f"{r.index:>2}  {r.config.at_s:>5.1f}  {r.intent_id or '-':<12}  {r.final_state:<17}  {domains:<26}  "
                         f"{len(r.attempts):>5}  {sla:<8}  {last_message}")
        return "\n".join(lines)


class ScenarioRunner:
    def __init__(self, scenario: Scenario):
        self.scenario = scenario
        self._t0 = None
        self._updates: dict[str, list[dict]] = {} # intent_id -> status updates
        self._log_files = {}

    def _now_s(self) -> float:
        return 0.0 if self._t0 is None else time.monotonic() - self._t0

    async def run(self) -> ScenarioResult:
        rt = self.scenario.runtime
        _prepare_transport(rt.transport)

        # Imported only now, so they bind to the mocked aiokafka/nats when transport is "mock".
        import nats
        import uvicorn
        from idmo_system.communication.message_bus_adapter import MessageBusAdapter
        from idmo_system.core.domain_capability_registration import DomainCapabilityRegistration
        from idmo_system.core.service_orchestrator import ServiceOrchestrator
        from idmo_system.core.sla_monitor import SLAMonitor
        from idmo_system.domains.core_dmo import CoreDomainBroker
        from idmo_system.domains.ran_dmo import RANDomainBroker
        from idmo_system.gateway.intent_gateway import create_app
        from idmo_system.gateway.translator import IntentTranslator
        from idmo_system.mocks.llm_mock import MockIntentTranslator

        log.info(f"SCENARIO: Starting '{self.scenario.name}' "
                 f"({len(self.scenario.domains)} domains, {len(self.scenario.intents)} intents, transport={rt.transport})")
        self._open_log_files()
        started = time.monotonic()
        tasks = []

        try:
            if rt.transport == "real":
                await self._cleanup_kafka_topics()

            registry = DomainCapabilityRegistration(
                nats_url=rt.nats_url,
                heartbeat_interval_s=self.scenario.registry.heartbeat_interval_s,
                max_missed_heartbeats=self.scenario.registry.max_missed_heartbeats,
            )
            components = [
                registry,
                SLAMonitor(nats_url=rt.nats_url),
                ServiceOrchestrator(nats_url=rt.nats_url, **self.scenario.orchestrator.model_dump()),
            ]
            tasks += [asyncio.create_task(c.run()) for c in components]
            tasks.append(asyncio.create_task(MessageBusAdapter(kafka_bootstrap=rt.kafka_bootstrap, nats_url=rt.nats_url).start()))

            nc = await nats.connect(rt.nats_url)
            await nc.subscribe("intent.status.update", cb=self._on_intent_update)
            tasks.append(asyncio.create_task(self._monitor_nats(nc)))
            tasks.append(asyncio.create_task(self._monitor_kafka()))

            if rt.llm == "mock":
                translator = MockIntentTranslator(self.scenario.translations)
            else:
                translator = IntentTranslator(llm_address=rt.llm_address, model=rt.llm_model)
            gateway = uvicorn.Server(uvicorn.Config(
                app=create_app(nats_url=rt.nats_url, translator=translator),
                host="127.0.0.1", port=rt.gateway_port, log_level="critical",
            ))
            gateway_task = asyncio.create_task(gateway.serve())
            tasks.append(gateway_task)

            for d in self.scenario.domains:
                broker_cls = RANDomainBroker if d.type == "ran" else CoreDomainBroker
                broker = broker_cls(domain_name=d.name, capabilities=d.capabilities,
                                    behavior=d.behavior, kafka_bootstrap=rt.kafka_bootstrap)
                tasks.append(asyncio.create_task(broker.start()))

            missing = await self._wait_for_registration(registry)
            while not gateway.started:
                await asyncio.sleep(0.05)

            self._t0 = time.monotonic()
            log.info("SCENARIO: t=0, system ready. Submitting intents.")
            results = [IntentResult(index=i, config=c) for i, c in enumerate(self.scenario.intents)]
            submitter = asyncio.create_task(self._submit_intents(results))
            tasks.append(submitter)

            while self._now_s() < rt.duration_s:
                self._refresh_results(results)
                if rt.stop_when_done and submitter.done() and all(r.is_terminal for r in results):
                    log.info("SCENARIO: All intents reached a terminal state.")
                    break
                await asyncio.sleep(0.2)
            else:
                log.info(f"SCENARIO: Reached duration limit ({rt.duration_s}s).")

            self._refresh_results(results)
            # Graceful stop releases the HTTP port (a cancelled server may keep it bound).
            gateway.should_exit = True
            await asyncio.wait({gateway_task}, timeout=5)
            await nc.close()

            registered = [d.name for d in self.scenario.domains if d.name not in missing]
            result = ScenarioResult(self.scenario.name, registered, missing, results, time.monotonic() - started)
            self._write_intent_log(result.summary())
            return result
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._close_log_files()

    async def _wait_for_registration(self, registry) -> list[str]:
        """Waits until every scenario domain is UP in the registry; returns names that did not get there in time."""
        expected = {d.name: registry._domain_id_from_name(d.name) for d in self.scenario.domains}
        deadline = time.monotonic() + self.scenario.runtime.registration_timeout_s
        while time.monotonic() < deadline:
            missing = [name for name, domain_id in expected.items()
                       if registry.domain_repository.get(domain_id, {}).get("status") != "UP"]
            if not missing:
                log.success(f"SCENARIO: All {len(expected)} domains registered.")
                return []
            await asyncio.sleep(0.1)
        log.error(f"SCENARIO: Domains not registered within timeout: {missing}")
        return missing

    async def _submit_intents(self, results: list[IntentResult]):
        url = f"http://127.0.0.1:{self.scenario.runtime.gateway_port}/intents"
        async with aiohttp.ClientSession() as session:
            for r in sorted(results, key=lambda r: r.config.at_s):
                await asyncio.sleep(max(0.0, r.config.at_s - self._now_s()))
                payload = {"intent_owner": r.config.owner}
                if r.config.rfs is not None:
                    payload["rfs"] = r.config.rfs.model_dump(exclude_none=True)
                else:
                    payload["expression"] = r.config.expression

                r.submitted_at_s = self._now_s()
                async with session.post(url, json=payload) as resp:
                    body = await resp.json()
                if resp.status == 201:
                    r.intent_id = body["intent_id"]
                    r.lifecycle_status = body.get("lifecycle_status")
                    log.info(f"SCENARIO: Intent #{r.index} submitted as {r.intent_id}")
                else:
                    r.error = f"HTTP {resp.status}: {body.get('detail')}"
                    log.warning(f"SCENARIO: Intent #{r.index} not accepted - {r.error}")
                self._write_intent_log(f"#{r.index} submitted: {payload} -> {r.intent_id or r.error}")

    async def _on_intent_update(self, msg):
        data = json.loads(msg.data.decode())
        intent_id = data.get("intent_id")
        if intent_id:
            self._updates.setdefault(intent_id, []).append({"t_s": round(self._now_s(), 3), **data})
            self._write_intent_log(f"{intent_id} update: {data}")

    def _refresh_results(self, results: list[IntentResult]):
        for r in results:
            if r.intent_id is None:
                continue
            r.history = list(self._updates.get(r.intent_id, []))
            for update in r.history:
                r.lifecycle_status = update.get("lifecycle_status", r.lifecycle_status)
                r.handling_state = update.get("current_handling_state", r.handling_state)

    # --- Infrastructure helpers ---------------------------------------------------------------

    async def _cleanup_kafka_topics(self):
        from aiokafka.admin import AIOKafkaAdminClient
        admin = AIOKafkaAdminClient(bootstrap_servers=self.scenario.runtime.kafka_bootstrap)
        await admin.start()
        try:
            to_delete = [t for t in await admin.list_topics() if t.startswith("unity6g.")]
            if to_delete:
                log.warning(f"SCENARIO: Cleaning up old topics: {to_delete}")
                await admin.delete_topics(to_delete)
                await asyncio.sleep(2) # Kafka needs a moment to physically delete the files
        except Exception as e:
            log.error(f"SCENARIO: Kafka cleanup failed: {e}")
        finally:
            await admin.close()

    async def _monitor_nats(self, nc):
        """Writes every NATS message to nats.log."""
        sub = await nc.subscribe(">")
        async for msg in sub.messages:
            self._write_log("nats", f"{msg.subject:<33} | {msg.data.decode('utf-8', errors='replace')}")

    async def _monitor_kafka(self):
        """Writes every Kafka message to kafka.log."""
        from aiokafka import AIOKafkaConsumer
        consumer = AIOKafkaConsumer(bootstrap_servers=self.scenario.runtime.kafka_bootstrap,
                                    group_id=f"scenario_monitor_{time.time_ns()}", auto_offset_reset="earliest")
        await consumer.start()
        try:
            consumer.subscribe(pattern=".*")
            async for msg in consumer:
                self._write_log("kafka", f"{msg.topic:<40} | {msg.value.decode('utf-8', errors='replace')}")
        finally:
            await consumer.stop()

    def _open_log_files(self):
        if self.scenario.runtime.output_dir is None:
            return
        out_dir = Path(self.scenario.runtime.output_dir) / re.sub(r"[^a-zA-Z0-9_-]", "_", self.scenario.name)
        out_dir.mkdir(parents=True, exist_ok=True)
        for name in ("nats", "kafka", "intents"):
            self._log_files[name] = open(out_dir / f"{name}.log", "w", encoding="utf-8")
        log.info(f"SCENARIO: Writing logs to {out_dir}")

    def _write_log(self, name: str, line: str):
        f = self._log_files.get(name)
        if f is not None:
            f.write(f"{datetime.now():%H:%M:%S} | {line}\n")
            f.flush()

    def _write_intent_log(self, line: str):
        self._write_log("intents", line)

    def _close_log_files(self):
        for f in self._log_files.values():
            f.close()
        self._log_files = {}
