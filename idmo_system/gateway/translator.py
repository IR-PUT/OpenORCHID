import aiohttp
import json
from loguru import logger as log

from idmo_system.communication.api import CoreRFS, RanRFS, ResourceFacingService

# Few-shot examples. The answers are serialized from the RFS models, so field names and their order always match
# the JSON schema the LLM output is constrained to (a different order in the examples confuses small models).
EXAMPLES = [
    ("We need a RAN with 100MHz bandwidth and 10ms latency. The core network should have 15ms latency and priority 1.",
     ResourceFacingService(ran=RanRFS(bandwidth_mhz=100, latency_ms=10), core=CoreRFS(priority=1, latency_ms=15))),
    ("Set up a slice for 20 seconds with core priority 2 and 50ms latency. RAN bandwidth must be 20 MHz in TA 1000.",
     ResourceFacingService(ran=RanRFS(bandwidth_mhz=20, tac=1000, duration_s=20), core=CoreRFS(priority=2, latency_ms=50))),
]


class IntentTranslator:
    def __init__(self, llm_address: str = "http://localhost:11434/api/generate", model: str = "qwen2.5-coder:1.5b"):
        self._llm_address = llm_address
        self._model = model

        examples = "\n\n".join(f"Input: {text}\nOutput: {rfs.model_dump_json()}" for text, rfs in EXAMPLES)
        self._prompt = (
            "You are a strict data extraction tool. Extract network requirements from the text into the exact JSON "
            "schema provided.\n"
            "Rules:\n"
            "- Respond ONLY with the raw JSON object.\n"
            "- Do NOT include markdown blocks (```json), formatting, or explanations.\n"
            "- If a value is missing, use null.\n\n"
            f"### Examples:\n{examples}\n\n"
            "### Input Text:\n"
        )

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
            except aiohttp.client_exceptions.ClientConnectorError as e:
                log.error(f"Translator: LLM not reachable at {self._llm_address}: {e}")
                return None

        if "error" in response:
            log.error(f"Translator: LLM error (model {self._model}): {response['error']}")
            return None

        try:
            result = json.loads(response["response"])
            return ResourceFacingService(**result).model_dump()
        except Exception as e:
            log.warning(f"Translator: LLM answer is not a valid RFS ({e}): {response.get('response')!r}")
            return None
