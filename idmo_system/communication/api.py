from typing import ClassVar
from enum import StrEnum

from pydantic import BaseModel


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
    cpu: int
    memory_gb: float
    storage_gb: float
    maxNumberofUEs: int


class UnityRanCapabilities(BaseModel):
    maxNumberofUEs: int
    nROperatingBands: list[str] # e.g. ['n78', 'n258']
    coverageAreaTAList: list[tuple[int, int, int]] # TAI - Tracking Area Identity (mcc, mnc, tac) 
    positioning: bool
    core_links: dict[str, float] = {}


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

