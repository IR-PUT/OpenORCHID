import aiohttp
import json

from idmo_system.communication.api import ResourceFacingService


class IntentTranslator:
    def __init__(self, llm_address: str = "http://localhost:11434/api/generate", model: str = "qwen2.5-coder:1.5b"):
        self._llm_address = llm_address
        self._model = model

        self._prompt = """You are a strict data extraction tool. Extract network requirements from the text into the exact JSON schema provided.
        Rules:
        - Respond ONLY with the raw JSON object.
        - Do NOT include markdown blocks (```json), formatting, or explanations.
        - If a value is missing, use null.

        ### Examples:
        Input: We need a RAN with 100MHz bandwidth and 10ms latency. The core network should have 15ms latency and priority 1.
        Output: {"ran": {"bandwidth_mhz": 100, "latency_ms": 10, "throughput_mbps": null, "num_ues": null, "tac": null, "duration_s": null}, "core": {"latency_ms": 15, "priority": 1}}

        Input: Set up a slice for 20 seconds with core priority 2 and 50ms latency. RAN bandwidth must be 20 MHz in TA 1000.
        Output: {"ran": {"bandwidth_mhz": 20, "latency_ms": null, "throughput_mbps": null, "num_ues": null, "tac": 1000, "duration_s": 20}, "core": {"latency_ms": 50, "priority": 2}}

        ### Input Text:
        """

    async def translate_to_rfs(self, expression: str) -> dict | None:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(self._llm_address, json={
                    "model": self._model,
                    "prompt": self._prompt + expression,
                    "format": ResourceFacingService.model_json_schema(),
                    "stream": False,
                    "options": {
                        "temperature": 0.01
                    }
                    }) as resp:
                    response = await resp.json()
            except aiohttp.client_exceptions.ClientConnectorError:
                return None

        try:
            result = json.loads(response["response"])
            return ResourceFacingService(**result).model_dump()
        except Exception:
            return None
