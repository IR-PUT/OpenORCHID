"""Scenario definition: which domains exist, how they behave and which intents are submitted.

A scenario is loaded from YAML (see scenarios/) and validated with the pydantic models below.
This module must not import aiokafka/nats, so it can be used before the mocks are installed.
"""

from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from idmo_system.communication.api import ResourceFacingService, UnityCoreCapabilities, UnityRanCapabilities
from idmo_system.domains.behavior import CoreBehavior, DomainEvent, RanBehavior


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RuntimeConfig(_StrictModel):
    duration_s: float = 60.0 # Hard limit of the scenario run, counted from the first intent slot (t=0)
    stop_when_done: bool = True # Finish early once every intent reached a terminal state
    transport: Literal["mock", "real"] = "mock" # mock = in-process Kafka/NATS, real = docker-compose services
    llm: Literal["mock", "ollama"] = "mock" # mock = answers from `translations`, ollama = real LLM
    kafka_bootstrap: str = "localhost:9092"
    nats_url: str = "nats://localhost:4222"
    llm_address: str = "http://localhost:11434/api/generate"
    llm_model: str = "qwen2.5-coder:1.5b"
    gateway_port: int = 8000
    registration_timeout_s: float = 30.0 # How long to wait for all domains to register before t=0
    output_dir: str | None = "results" # Logs and report.json go to <output_dir>/<scenario name>/; None disables files
    seed: int | None = None # Makes "random" telemetry reproducible


class OrchestratorConfig(_StrictModel):
    domain_selection: str = "first_available" # Strategy from core/domain_selection.py
    max_candidates: int = Field(default=3, ge=1) # How many (RAN, CORE) pairs to try before rejecting
    feasibility_timeout_s: float = 5.0 # A domain that does not answer in time counts as a rejection
    seed: int | None = None # Seed for the "random" strategy

    @model_validator(mode="after")
    def _known_strategy(self):
        from idmo_system.core.domain_selection import STRATEGIES
        if self.domain_selection not in STRATEGIES:
            raise ValueError(f"unknown domain_selection '{self.domain_selection}', available: {sorted(STRATEGIES)}")
        return self


class RegistryConfig(_StrictModel):
    heartbeat_interval_s: float = 10.0
    max_missed_heartbeats: int = 3


class _DomainConfigBase(_StrictModel):
    name: str
    # Own broker implementation, "package.module:ClassName" (or "package.module.ClassName"). It must subclass
    # RANDomainBroker / CoreDomainBroker; it is imported by the runner, after the transport mocks are installed.
    class_path: str | None = Field(default=None, alias="class", pattern=r"^[A-Za-z_][\w.]*([:.][A-Za-z_]\w*)$")
    params: dict[str, Any] = {} # Extra keyword arguments for the broker constructor


class RanDomainConfig(_DomainConfigBase):
    type: Literal["ran"]
    capabilities: UnityRanCapabilities
    behavior: RanBehavior = RanBehavior()


class CoreDomainConfig(_DomainConfigBase):
    type: Literal["core"]
    capabilities: UnityCoreCapabilities
    behavior: CoreBehavior = CoreBehavior()


DomainConfig = Annotated[RanDomainConfig | CoreDomainConfig, Field(discriminator="type")]


class IntentConfig(_StrictModel):
    at_s: float = 0.0 # Submission time, relative to t=0 (all domains registered)
    owner: str = "Anonymous"
    expression: str | None = None # Natural language, translated by the LLM
    rfs: ResourceFacingService | None = None # Structured request, bypasses the LLM

    @model_validator(mode="after")
    def _expression_or_rfs(self):
        if (self.expression is None) == (self.rfs is None):
            raise ValueError("intent needs exactly one of 'expression' or 'rfs'")
        return self


FinalState = Literal["HANDLING_ENDED", "REJECTED", "SUBMISSION_FAILED", "STATE_COMPLIES", "STATE_DEGRADES",
                     "IDLE", "NOT_SUBMITTED", "UNKNOWN"]


class ExpectConfig(_StrictModel):
    """Assertion checked after the run; every given field must match."""
    intent: int # Index in `intents`
    final_state: FinalState | None = None
    ran: str | None = None # Domain the service was provisioned on
    core: str | None = None
    attempts: int | None = None # Number of (RAN, CORE) candidates asked for feasibility
    sla_degraded: bool | None = None # SLA degradation reported at any point
    message_contains: str | None = None # Substring of the last status message


class Scenario(_StrictModel):
    name: str
    description: str = ""
    runtime: RuntimeConfig = RuntimeConfig()
    registry: RegistryConfig = RegistryConfig()
    orchestrator: OrchestratorConfig = OrchestratorConfig()
    domains: list[DomainConfig] = Field(min_length=1)
    intents: list[IntentConfig] = []
    events: list[DomainEvent] = []
    expect: list[ExpectConfig] = []
    translations: dict[str, ResourceFacingService] = {} # Mock LLM: expression -> RFS

    @model_validator(mode="after")
    def _check_references(self):
        names = [d.name for d in self.domains]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"domain names must be unique, duplicated: {duplicates}")

        core_names = {d.name for d in self.domains if d.type == "core"}
        for domain in self.domains:
            if domain.type == "ran":
                unknown = set(domain.capabilities.core_links) - core_names
                if unknown:
                    raise ValueError(f"domain '{domain.name}': core_links refer to unknown CORE domains {sorted(unknown)}")

        domain_types = {d.name: d.type for d in self.domains}
        for event in self.events:
            if event.domain not in domain_types:
                raise ValueError(f"event at {event.at_s}s refers to unknown domain '{event.domain}'")
            if event.action in ("degrade", "restore") and domain_types[event.domain] != "ran":
                raise ValueError(f"event '{event.action}' is only supported for RAN domains, not '{event.domain}'")

        for e in self.expect:
            if not 0 <= e.intent < len(self.intents):
                raise ValueError(f"expect refers to intent {e.intent}, but there are {len(self.intents)} intents (0-based)")
            for name in (e.ran, e.core):
                if name is not None and name not in domain_types:
                    raise ValueError(f"expect for intent {e.intent} refers to unknown domain '{name}'")
        return self

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Scenario":
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
