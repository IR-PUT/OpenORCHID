"""Scenario definition: which domains exist, how they behave and which intents are submitted.

A scenario is loaded from YAML (see scenarios/) and validated with the pydantic models below.
This module must not import aiokafka/nats, so it can be used before the mocks are installed.
"""

from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from idmo_system.communication.api import ResourceFacingService, UnityCoreCapabilities, UnityRanCapabilities
from idmo_system.domains.behavior import CoreBehavior, RanBehavior


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
    output_dir: str | None = "results" # Logs go to <output_dir>/<scenario name>/; None disables file logs


class RegistryConfig(_StrictModel):
    heartbeat_interval_s: float = 10.0
    max_missed_heartbeats: int = 3


class RanDomainConfig(_StrictModel):
    name: str
    type: Literal["ran"]
    capabilities: UnityRanCapabilities
    behavior: RanBehavior = RanBehavior()


class CoreDomainConfig(_StrictModel):
    name: str
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


class Scenario(_StrictModel):
    name: str
    description: str = ""
    runtime: RuntimeConfig = RuntimeConfig()
    registry: RegistryConfig = RegistryConfig()
    domains: list[DomainConfig] = Field(min_length=1)
    intents: list[IntentConfig] = []
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
        return self

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Scenario":
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
