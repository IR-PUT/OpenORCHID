"""Configurable behaviour of simulated domains (what a domain answers, what telemetry it reports)
and the events that can be injected into a running domain."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class TelemetryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interval_s: float = 5.0
    throughput_mbps: float = 85.5
    latency_ms: float = 12.0
    # constant: always the values above; random: values +/- jitter; degrading: worse with every report
    profile: Literal["constant", "random", "degrading"] = "constant"
    jitter: float = Field(default=0.1, ge=0, lt=1) # random: relative deviation, 0.1 = +/-10%
    degradation_per_report: float = Field(default=0.05, ge=0, lt=1) # degrading: relative loss per report


class RanBehavior(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_bandwidth_mhz: float = 4000.0 # Total bandwidth capacity, shared by all active services
    telemetry: TelemetryConfig = TelemetryConfig()


class CoreBehavior(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_services: int | None = None # Max number of concurrent services, None = unlimited


class DomainEvent(BaseModel):
    """Something that happens to a domain during a scenario."""
    model_config = ConfigDict(extra="forbid")

    at_s: float # Relative to t=0 (all domains registered)
    domain: str
    # stop_heartbeat: the domain keeps working but stops sending heartbeats (registry declares it lost)
    # crash:          the domain stops completely (heartbeats, requests, telemetry)
    # degrade:        RAN telemetry multiplied by the factors below; restore: back to normal
    action: Literal["stop_heartbeat", "crash", "degrade", "restore"]
    throughput_factor: float = Field(default=1.0, ge=0)
    latency_factor: float = Field(default=1.0, ge=0)
