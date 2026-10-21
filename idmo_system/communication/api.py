from typing import ClassVar
from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class BaseMessage(BaseModel):
    kafka_topic: ClassVar[str] = None
    message_type: str
    source_module: str
    target_module: str

    @classmethod
    def get_topic(cls) -> str:
        return cls.kafka_topic


class DomainType(StrEnum):
    RAN = 'ran'
    CORE = 'core'


class UnityCoreCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu: int
    memory_gb: float
    storage_gb: float
    maxNumberofUEs: int


class UnityRanCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid")

    maxNumberofUEs: int
    nROperatingBands: list[str] # e.g. ['n78', 'n258']
    coverageAreaTAList: list[tuple[int, int, int]] # TAI - Tracking Area Identity (mcc, mnc, tac) 
    positioning: bool
    core_links: dict[str, float] = {} # CORE domain name -> link latency [ms]


class RanRFS(BaseModel):
    """Resource Facing Service requested from a RAN domain. Missing values (None) are not constrained."""
    model_config = ConfigDict(extra="forbid")

    bandwidth_mhz: float | None = None
    latency_ms: float | None = None
    throughput_mbps: float | None = None
    num_ues: int | None = None
    tac: int | None = None # Tracking Area Code where the service is needed
    duration_s: float | None = None


class CoreRFS(BaseModel):
    """Resource Facing Service requested from a CORE domain."""
    model_config = ConfigDict(extra="forbid")

    priority: int | None = None
    latency_ms: float | None = None # Max RAN <-> CORE link latency


class ResourceFacingService(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ran: RanRFS = RanRFS()
    core: CoreRFS = CoreRFS()


class DomainRegistrationRequest(BaseMessage):
    kafka_topic = 'unity6g.domain.registration.request'
    message_type: str = 'DomainRegistrationRequest'
    domain_name: str
    domain_type: DomainType
    capabilities: UnityCoreCapabilities | UnityRanCapabilities


class DomainRegistrationResponse(BaseMessage):
    kafka_topic = 'unity6g.domain.registration.response'
    message_type: str = 'DomainRegistrationResponse'
    status: bool
    message: str | None = None

    domain_unique_id: str | None = None
    topic_domain_to_idmo: str | None = None
    topic_idmo_to_domain: str | None = None
    heartbeat_interval_s: float | None = None
    

class DomainHealthMessage(BaseMessage):
    message_type: str = 'DomainHealthMessage'
    status: bool
    message: str | None = None

