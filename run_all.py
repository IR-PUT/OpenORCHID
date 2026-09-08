import idmo_system.mock_kafka
import idmo_system.mock_nats
import idmo_system.mock_llm

import aiohttp
import asyncio
import json
import logging
import sys
import uuid
from pathlib import Path
from loguru import logger
from nats.aio.client import Client as NATS

from idmo_system.communication.message_bus_adapter import MessageBusAdapter
from idmo_system.core.sla_monitor import SLAMonitor
from idmo_system.core.service_orchestrator import ServiceOrchestrator
from idmo_system.core.domain_capability_registration import DomainCapabilityRegistration
from idmo_system.domains.core_dmo import CoreDomainBroker
from idmo_system.domains.ran_dmo import RANDomainBroker
from idmo_system.gateway.intent_gateway import run_gateway_server
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient


NATS_LOG_FILE = Path("results") / "nats_all_subjects.log"
KAFKA_LOG_FILE = Path("results") / "kafka_all_topics.log"
USER_LOG_FILE = Path("results") / "user_intent.log"
SHUTDOWN_AFTER_SECONDS = 60


def disable_fastapi_logs():
    """Silence uvicorn/FastAPI logs in run_all orchestration mode."""
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        std_logger = logging.getLogger(logger_name)
        std_logger.handlers.clear()
        std_logger.propagate = False
        std_logger.disabled = True


def configure_loggers():
    """Configure system logs to console and monitor logs to dedicated files only."""
    logger.remove()

    # Keep normal system logs on console.
    logger.add(
        sys.stderr,
        level="INFO",
        filter=lambda record: (
            not record["extra"].get("nats_file_only", False)
            and not record["extra"].get("kafka_file_only", False)
            and not record["extra"].get("user_file_only", False)
        ),
    )

    # Route NATS monitor logs only to file.
    NATS_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(NATS_LOG_FILE),
        # format="{H}:{M}:{S} | {subject:<30} | {message}",
        format="{time:HH:mm:ss} | {extra[subject]:<33} | {message}",
        level="DEBUG",
        mode='w',
        # rotation="10 MB",
        enqueue=True,
        filter=lambda record: record["extra"].get("nats_file_only", False),
    )

    # Route Kafka monitor logs only to file.
    KAFKA_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(KAFKA_LOG_FILE),
        format="{time:HH:mm:ss} | {extra[topic]:<40} | {message}",
        level="DEBUG",
        mode='w',
        enqueue=True,
        filter=lambda record: record["extra"].get("kafka_file_only", False),
    )

    USER_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(USER_LOG_FILE),
        format="{time:HH:mm:ss} | {message}",
        level="DEBUG",
        mode='w',
        enqueue=True,
        filter=lambda record: record["extra"].get("user_file_only", False),
    )


nats_file_logger = logger.bind(nats_file_only=True)
kafka_file_logger = logger.bind(kafka_file_only=True)
user_file_logger = logger.bind(user_file_only=True)






async def simulate_user_intent(nc):
    """
    Simulates the Intent Gateway receiving an intent and 
    starting the chain reaction.
    """
    await asyncio.sleep(5) # Wait for registration to complete
    
    intent_payload = {
        "intent_id": "INT-001",
        "rfs_ran": {"bandwidth": 100, "latency": 10},
        "rfs_core": {"latency": 15, "priority": 1}
    }
    
    logger.info("USER: Sending new Service Order Request...")
    await nc.publish("service.order.request", json.dumps(intent_payload).encode())

async def simulate_user_intent_with_status_check():
    await asyncio.sleep(5)  # Wait for the system to be ready
    user_file_logger.info("Starting intent simulation with status polling...")
    url = "http://localhost:8000/"

    async with aiohttp.ClientSession() as session:
        data = {
            "expression": "Set up a slice with core priority 2 and 50ms latency. RAN bandwidth must be 20."
        }
        user_file_logger.info(f"Submitting intent: {data}")
        async with session.post(url + "intents", json=data) as resp:
            response_data = await resp.json()
            user_file_logger.info(f"Intent submission response: {response_data}")

        intent_id = response_data['intent_id']


        # Step 2: Poll for status updates
        while True:
            await asyncio.sleep(2)  # Poll every 2 seconds
            async with session.get(url + f"intents/{intent_id}") as status_resp:
                status_data = await status_resp.json()
                user_file_logger.info(f"Intent status update: {status_data}")
                if status_data.get("lifecycle_status") in ["ARCHIVED"]:
                    break

    user_file_logger.info("Finished intent simulation with status polling.")


async def monitor_system_events():
    """Subscribe to all NATS subjects and persist every message to file."""
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    try:
        sub_all = await nc.subscribe(">")

        # nats_file_logger.info("NATS monitor started: subscribed to all subjects ('>').")

        async for msg in sub_all.messages:
            try:
                payload_text = msg.data.decode("utf-8")
            except UnicodeDecodeError:
                payload_text = repr(msg.data)

            nats_file_logger.bind(subject=msg.subject).info(f"{payload_text}")
    finally:
        await nc.close()


async def monitor_kafka_events(bootstrap_servers):
    """Subscribe to all Kafka topics and persist every message to file."""
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=f"kafka_monitor_{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        consumer.subscribe(pattern=".*")

        async for msg in consumer:
            try:
                payload_text = msg.value.decode("utf-8")
            except UnicodeDecodeError:
                payload_text = repr(msg.value)

            kafka_file_logger.bind(topic=msg.topic).info(f"{payload_text}")
    finally:
        await consumer.stop()

async def cleanup_kafka(bootstrap_servers):
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        existing_topics = await admin.list_topics()
        to_delete = [t for t in existing_topics if t.startswith("unity6g.")]
        
        if to_delete:
            logger.warning(f"SYSTEM: Cleaning up old topics: {to_delete}")
            await admin.delete_topics(to_delete)
            # Kafka potrzebuje chwili na fizyczne usunięcie plików
            await asyncio.sleep(2) 
    except Exception as e:
        logger.error(f"SYSTEM: Cleanup failed: {e}")
    finally:
        await admin.close()
        
async def main():    
    configure_loggers()
    disable_fastapi_logs()

    bootstrap = "localhost:9092"

    await cleanup_kafka(bootstrap)

    registry = DomainCapabilityRegistration()
    sla_monitor = SLAMonitor()
    adapter = MessageBusAdapter(kafka_bootstrap=bootstrap)

    reg_task = asyncio.create_task(registry.run())
    sm_task = asyncio.create_task(sla_monitor.run())
    adapter_task = asyncio.create_task(adapter.start())

    logger.info("SYSTEM: Waiting for registry startup...")
    # await asyncio.sleep(5) 

    orchestrator = ServiceOrchestrator()
    ran_domain = RANDomainBroker(kafka_bootstrap=bootstrap)
    core_domain = CoreDomainBroker(kafka_bootstrap=bootstrap)

    ran_task = asyncio.create_task(ran_domain.start())
    core_task = asyncio.create_task(core_domain.start())

    logger.info("SYSTEM: Domains sent requests. Waiting for Registry to create their topics...")
    # await asyncio.sleep(5) 
    intent_gateway = asyncio.create_task(
        run_gateway_server(host="0.0.0.0", port=8000, log_level="critical")
    )

    logger.info("SYSTEM: Everything ready. Starting intent simulation.")

    worker_tasks = [
        reg_task,
        sm_task,
        adapter_task,
        asyncio.create_task(orchestrator.run()),
        ran_task,
        core_task,
        intent_gateway,
        asyncio.create_task(simulate_user_intent_with_status_check()),
        asyncio.create_task(monitor_system_events()),
        asyncio.create_task(monitor_kafka_events(bootstrap)),
    ]

    try:
        await asyncio.sleep(SHUTDOWN_AFTER_SECONDS)
        logger.info(f"SYSTEM: Reached {SHUTDOWN_AFTER_SECONDS}s runtime limit. Shutting down gracefully...")
    except Exception as e:
        logger.error(f"SYSTEM Error: {e}")
    finally:
        for task in worker_tasks:
            task.cancel()

        await asyncio.gather(*worker_tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())