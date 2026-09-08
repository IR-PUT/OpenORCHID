import asyncio
import json
import uuid
import nats
import uvicorn
from fastapi import FastAPI, HTTPException
from datetime import datetime
from loguru import logger as log

from idmo_system.gateway.translator import IntentTranslator

app = FastAPI(title="IDMO Intent Gateway")

intent_state_repository = {}

async def run_gateway_server(host: str = "0.0.0.0", port: int = 8000, log_level: str = "info"):
    """
    Async entrypoint for running this FastAPI app via Uvicorn.
    Can be imported and scheduled with asyncio.create_task(...)
    """
    config = uvicorn.Config(app=app, host=host, port=port, log_level=log_level)
    server = uvicorn.Server(config)
    await server.serve()

def start_gateway_server_task(host: str = "0.0.0.0", port: int = 8000, log_level: str = "info") -> asyncio.Task:
    """
    Convenience helper for callers that already run an event loop.
    """
    return asyncio.create_task(
        run_gateway_server(host=host, port=port, log_level=log_level),
        name="intent-gateway-server"
    )

async def get_nats_conn():
    """Establishes connection to the Internal Bus (NATS)."""
    return await nats.connect("nats://localhost:4222")

@app.on_event("startup")
async def startup_event():
    log.info("SYSTEM: Gateway is starting up...")
    
    task = asyncio.create_task(listen_for_intent_updates())
    app.state.intent_listener_task = task
    
    def task_done_callback(t):
        try:
            t.result()
        except asyncio.CancelledError:
            log.info("SYSTEM: Background listener cancelled during shutdown.")
        except Exception as e:
            log.critical(f"SYSTEM: Background listener DIED with error: {e}")
            
    task.add_done_callback(task_done_callback)


@app.on_event("shutdown")
async def shutdown_event():
    task = getattr(app.state, "intent_listener_task", None)
    if task is None:
        return

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

@app.post("/intents", status_code=201)
async def create_intent(payload: dict):
    global intent_state_repository
    """
    1. Generates a unique intent_id.
    2. Directly invokes the Translator (LLM) to map natural language to RFS.
    3. Publishes ServiceOrderRequest (containing RAN & Core RFS) to the Saga.
    4. Returns the confirmation to the Intent Owner.
    """
    
    # --- STEP 1: Generate Intent ID and Validate Input ---
    # Creating a traceable ID used across Saga, Kafka, and Databases
    intent_id = f"INT-{uuid.uuid4().hex[:8].upper()}" 
    expression = payload.get("expression")
    owner = payload.get("intent_owner", "Anonymous")

    if not expression:
        raise HTTPException(status_code=400, detail="Intent expression is required")

    # --- STEP 2: Initialize record in Intent State Repository (ISR) ---
    # Providing immediate visibility for the Intent Owner
    intent_state_repository[intent_id] = {
        "intent_id": intent_id,
        "owner": owner,
        "expression": expression,
        "lifecycle_status": "CREATED",
        "current_handling_state": "IDLE",
        "created_at": datetime.utcnow().isoformat()
    }

    # --- STEP 3: Direct Translator (LLM) Invocation ---
    # The Translator extracts technical parameters for both network domains
    try:
        translator = IntentTranslator()
        rfs_data = await translator.translate_to_rfs(expression)
        if rfs_data is None:
            raise ValueError("Translator returned no data for the given expression.")
        # print(rfs_data)
        # rfs_data = {
        #     "ran": {
        #         "slice_type": "URLLC",
        #         "duration_s": 30,
        #         "priority": 1,
        #         "throughput_mbps": 50,
        #         "latency_target_ms": 20,
        #         "resource_blocks": 50
        #     },
        #     "core": {
        #         "duration_s": 30,
        #         "snssai": "01-AABBCC",
        #         "dnn": "robotics.internal",
        #         "upf_node": "edge-upf-01"
        #     }
        # }
        # rfs_data = {
        #     "ran": {"bandwidth": 100, "latency": 10},
        #     "core": {"latency": 15, "priority": 1}
        # }

    except Exception as e:
        intent_state_repository[intent_id]["current_handling_state"] = "IntentRejected"
        raise HTTPException(status_code=422, detail=f"Translation Error: {str(e)}")

    # --- STEP 4: Publish ServiceOrderRequest to the Saga Orchestrator ---
    # This event triggers the Preparation Phase (Feasibility Check)
    try:
        nc = await get_nats_conn()
        
        # Payload contains both RFS models and the unique Intent ID
        event_payload = {
            "intent_id": intent_id,
            "source": "Translator",
            "timestamp": datetime.utcnow().isoformat(),
            "rfs_ran": rfs_data["ran"],
            "rfs_core": rfs_data["core"]
        }

        # Dispatching to the Saga via Internal Bus (NATS)
        await nc.publish("service.order.request", json.dumps(event_payload).encode())
        
        # await nc.flush()
        # await nc.close()
    except Exception as e:
        # Rollback or update status if the internal bus is unreachable
        intent_state_repository[intent_id]["current_handling_state"] = "IntentRejected"
        raise HTTPException(status_code=500, detail="Failed to reach Internal Bus")

    # --- STEP 5: Final HTTP Response ---
    # The request is accepted and the orchestration lifecycle has begun
    return {
        "intent_id": intent_id,
        "lifecycle_status": "CREATED",
        "message": "Intent translated and ServiceOrderRequest dispatched to Saga Orchestrator."#  + json.dumps(rfs_data)
    }

async def listen_for_intent_updates():
    global intent_state_repository
    nc = await get_nats_conn()
    sub = await nc.subscribe("intent.status.update")
    log.info("Gateway ISR: Listening for all lifecycle updates...")

    async for msg in sub.messages:
        try:
            data = json.loads(msg.data.decode())
            intent_id = data.get("intent_id")
            
            
            if intent_id in intent_state_repository:
                
                new_lifecycle = data.get("lifecycle_status", intent_state_repository[intent_id].get("lifecycle_status"))
                new_handling = data.get("current_handling_state", intent_state_repository[intent_id].get("current_handling_state"))

                intent_state_repository[intent_id].update({
                    "lifecycle_status": new_lifecycle,
                    "current_handling_state": new_handling,
                    "last_message": data.get("message", "No extra info"),
                    "updated_at": datetime.utcnow().isoformat()
                })
                
                log.info(f"ISR: Intent {intent_id} transitioned to state: {new_lifecycle} and {new_handling}")
                
                if new_handling in ["HANDLING_ENDED", "REJECTED"]:
                    intent_state_repository[intent_id].update({
                        "lifecycle_status": "ARCHIVED"
                    })
                    log.warning(f"ISR: Intent {intent_id} reached terminal state: {new_handling}")

        except Exception as e:
            log.error(f"ISR: Update error: {e}")

@app.get("/intents/{intent_id}")
async def get_intent_status(intent_id: str):
    """Retrieves the current intent status from the ISR database."""
    if intent_id not in intent_state_repository:
        raise HTTPException(status_code=404, detail="Intent not found in ISR")
    return intent_state_repository[intent_id]

@app.get("/intents")
async def get_all_intents():
    return intent_state_repository

if __name__ == "__main__":
    asyncio.run(run_gateway_server(host="0.0.0.0", port=8000))