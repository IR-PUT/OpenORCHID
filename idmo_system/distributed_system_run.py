import asyncio
from loguru import logger
from nats.aio.client import Client as NATS

from idmo_system.communication.message_bus_adapter import MessageBusAdapter
from idmo_system.core.sla_monitor import SLAMonitor
from idmo_system.core.service_orchestrator import ServiceOrchestrator
from idmo_system.core.domain_capability_registration import DomainCapabilityRegistration
from idmo_system.domains.core_dmo import CoreDomainBroker
from idmo_system.domains.ran_dmo import RANDomainBroker
from aiokafka.admin import AIOKafkaAdminClient

async def monitor_system_events():
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    sub_all = await nc.subscribe(">")
    logger.info("MONITOR: Watching ALL system events on NATS...")

    async for msg in sub_all.messages:
        try:
            logger.debug(f"NATS EVENT: {msg.subject}")
        except Exception:
            pass

async def main():
    bootstrap = "localhost:9092"
    await cleanup_kafka(bootstrap)
    
    registry = DomainCapabilityRegistration()
    sla_monitor = SLAMonitor()
    adapter = MessageBusAdapter(kafka_bootstrap=bootstrap)
    orchestrator = ServiceOrchestrator()

    ran_domain = RANDomainBroker(kafka_bootstrap=bootstrap)
    core_domain = CoreDomainBroker(kafka_bootstrap=bootstrap)

    logger.info("SYSTEM: Starting Distributed Backend Components...")

    try:
        await asyncio.gather(
            registry.run(),             # Domain Registration
            sla_monitor.run(),          # SLA Monitoring
            adapter.start(),            # Kafka-NATS adapter
            orchestrator.run(),         # Service Orchestrator
            ran_domain.start(),         # One RAN domain
            core_domain.start(),        # One core domain
            monitor_system_events()     # Event monitoring
        )
    except Exception as e:
        logger.error(f"SYSTEM FATAL ERROR: {e}")

async def cleanup_kafka(bootstrap_servers):
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:

        existing_topics = await admin.list_topics()
        to_delete = [t for t in existing_topics if t.startswith("unity6g.")]
        
        if to_delete:
            logger.warning(f"SYSTEM: Cleaning up old topics: {to_delete}")
            await admin.delete_topics(to_delete)
            await asyncio.sleep(2) 
    except Exception as e:
        logger.error(f"SYSTEM: Cleanup failed: {e}")
    finally:
        await admin.close()

if __name__ == "__main__":
    asyncio.run(main())