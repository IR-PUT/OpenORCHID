"""Configurable behaviour of simulated domains (what a domain answers, what telemetry it reports)."""

from pydantic import BaseModel, ConfigDict


class TelemetryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interval_s: float = 5.0
    throughput_mbps: float = 85.5
    latency_ms: float = 12.0


class RanBehavior(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_bandwidth_mhz: float = 4000.0 # Total bandwidth capacity, shared by all active services
    telemetry: TelemetryConfig = TelemetryConfig()


class CoreBehavior(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_services: int | None = None # Max number of concurrent services, None = unlimited
