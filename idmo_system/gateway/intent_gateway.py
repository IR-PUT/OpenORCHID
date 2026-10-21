import asyncio
import json
import uuid
import nats
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from datetime import datetime, timezone
from loguru import logger as log

from idmo_system.communication.api import ResourceFacingService
from idmo_system.gateway.translator import IntentTranslator


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_app(nats_url: str = "nats://localhost:4222", translator: IntentTranslator | None = None) -> FastAPI:
    """
    Builds the Intent Gateway app. Each app has its own NATS connection and
    Intent State Repository (ISR), kept in app.state.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("SYSTEM: Gateway is starting up...")
        app.state.nc = await nats.connect(nats_url)
        app.state.intent_state_repository = {}
        app.state.translator = translator or IntentTranslator()

        task = asyncio.create_task(listen_for_intent_updates(app))

        def task_done_callback(t):
            try:
                t.result()
            except asyncio.CancelledError:
                log.info("SYSTEM: Background listener cancelled during shutdown.")
            except Exception as e:
                log.critical(f"SYSTEM: Background listener DIED with error: {e}")

        task.add_done_callback(task_done_callback)

        yield

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await app.state.nc.close()

    app = FastAPI(title="IDMO Intent Gateway", lifespan=lifespan)

    @app.post("/intents", status_code=201)
    async def create_intent(payload: dict, request: Request):
        """
        Payload: {"expression": "<natural language>"} and/or {"rfs": {"ran": {...}, "core": {...}}},
        optional "intent_owner". A structured "rfs" bypasses the Translator.

        1. Generates a unique intent_id.
        2. Directly invokes the Translator (LLM) to map natural language to RFS.
        3. Publishes ServiceOrderRequest (containing RAN & Core RFS) to the Saga.
        4. Returns the confirmation to the Intent Owner.
        """
        state = request.app.state
        isr = state.intent_state_repository

        # --- STEP 1: Generate Intent ID and Validate Input ---
        # Creating a traceable ID used across Saga, Kafka, and Databases
        intent_id = f"INT-{uuid.uuid4().hex[:8].upper()}"
        expression = payload.get("expression")
        structured_rfs = payload.get("rfs")
        owner = payload.get("intent_owner", "Anonymous")

        if not expression and not structured_rfs:
            raise HTTPException(status_code=400, detail="Intent expression or rfs is required")

        # --- STEP 2: Initialize record in Intent State Repository (ISR) ---
        # Providing immediate visibility for the Intent Owner
        isr[intent_id] = {
            "intent_id": intent_id,
            "owner": owner,
            "expression": expression,
            "lifecycle_status": "CREATED",
            "current_handling_state": "IDLE",
            "created_at": _now()
        }

        # --- STEP 3: Direct Translator (LLM) Invocation ---
        # The Translator extracts technical parameters for both network domains
        try:
            if structured_rfs is not None:
                rfs_data = ResourceFacingService.model_validate(structured_rfs).model_dump()
            else:
                rfs_data = await state.translator.translate_to_rfs(expression)
            if rfs_data is None:
                raise ValueError("Translator returned no data for the given expression.")
        except Exception as e:
            isr[intent_id]["current_handling_state"] = "IntentRejected"
            raise HTTPException(status_code=422, detail=f"Translation Error: {str(e)}")

        # --- STEP 4: Publish ServiceOrderRequest to the Saga Orchestrator ---
        # This event triggers the Preparation Phase (Feasibility Check)
        try:
            # Payload contains both RFS models and the unique Intent ID
            event_payload = {
                "intent_id": intent_id,
                "source": "Translator",
                "timestamp": _now(),
                "rfs_ran": rfs_data["ran"],
                "rfs_core": rfs_data["core"]
            }

            # Dispatching to the Saga via Internal Bus (NATS)
            await state.nc.publish("service.order.request", json.dumps(event_payload).encode())
        except Exception:
            # Rollback or update status if the internal bus is unreachable
            isr[intent_id]["current_handling_state"] = "IntentRejected"
            raise HTTPException(status_code=500, detail="Failed to reach Internal Bus")

        # --- STEP 5: Final HTTP Response ---
        # The request is accepted and the orchestration lifecycle has begun
        return {
            "intent_id": intent_id,
            "lifecycle_status": "CREATED",
            "message": "Intent translated and ServiceOrderRequest dispatched to Saga Orchestrator."
        }

    @app.get("/intents/{intent_id}")
    async def get_intent_status(intent_id: str, request: Request):
        """Retrieves the current intent status from the ISR database."""
        isr = request.app.state.intent_state_repository
        if intent_id not in isr:
            raise HTTPException(status_code=404, detail="Intent not found in ISR")
        return isr[intent_id]

    @app.get("/intents")
    async def get_all_intents(request: Request):
        return request.app.state.intent_state_repository

    return app


async def listen_for_intent_updates(app: FastAPI):
    isr = app.state.intent_state_repository
    sub = await app.state.nc.subscribe("intent.status.update")
    log.info("Gateway ISR: Listening for all lifecycle updates...")

    async for msg in sub.messages:
        try:
            data = json.loads(msg.data.decode())
            intent_id = data.get("intent_id")

            if intent_id in isr:
                new_lifecycle = data.get("lifecycle_status", isr[intent_id].get("lifecycle_status"))
                new_handling = data.get("current_handling_state", isr[intent_id].get("current_handling_state"))

                isr[intent_id].update({
                    "lifecycle_status": new_lifecycle,
                    "current_handling_state": new_handling,
                    "last_message": data.get("message", "No extra info"),
                    "updated_at": _now()
                })

                log.info(f"ISR: Intent {intent_id} transitioned to state: {new_lifecycle} and {new_handling}")

                if new_handling in ["HANDLING_ENDED", "REJECTED"]:
                    isr[intent_id].update({
                        "lifecycle_status": "ARCHIVED"
                    })
                    log.warning(f"ISR: Intent {intent_id} reached terminal state: {new_handling}")

        except Exception as e:
            log.error(f"ISR: Update error: {e}")


async def run_gateway_server(host: str = "0.0.0.0", port: int = 8000, log_level: str = "info",
                             nats_url: str = "nats://localhost:4222", translator: IntentTranslator | None = None):
    """
    Async entrypoint for running the gateway via Uvicorn.
    Can be imported and scheduled with asyncio.create_task(...)
    """
    config = uvicorn.Config(app=create_app(nats_url, translator), host=host, port=port, log_level=log_level)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(run_gateway_server(host="0.0.0.0", port=8000))
