from pydantic import BaseModel
import aiohttp
import json


class RanResourceFacingService(BaseModel):
    bandwidth_mhz: float
    latency_ms: float


class CoreResourceFacingService(BaseModel):
    priority: int
    latency_ms: float


class ResourceFacingService(BaseModel):
    ran: RanResourceFacingService | None = None 
    core: CoreResourceFacingService | None = None 


class IntentTranslator:
    def __init__(self, llm_address: str = "http://localhost:11434/api/generate"):
        self._llm_address = llm_address
        self._model = "qwen2.5-coder:1.5b"

        self._prompt = """You are a strict data extraction tool. Extract network requirements from the text into the exact JSON schema provided. 
        Rules:
        - Respond ONLY with the raw JSON object.
        - Do NOT include markdown blocks (```json), formatting, or explanations.
        - If a value is missing, use null.

        ### Examples:
        Input: We need a RAN with 100MHz bandwidth and 10ms latency. The core network should have 15ms latency and priority 1.
        Output: {"ran": {"bandwidth": 100, "latency": 10}, "core": {"latency": 15, "priority": 1}}

        Input: Set up a slice with core priority 2 and 50ms latency. RAN bandwidth must be 20.
        Output: {"ran": {"bandwidth": 20, "latency": null}, "core": {"latency": 50, "priority": 2}}

        ### Input Text:
        """


    async def translate_to_rfs(self, expression: str) -> dict:
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
            except aiohttp.client_exceptions.ClientConnectorError as ex: 
                return None

        specification = None
        try:
            result = json.loads(response["response"])
            specification = ResourceFacingService(**result)
        except Exception as e:
            return None

        return specification.model_dump() if specification else None
        return {
            "ran": {"bandwidth": 100, "latency": 10},
            "core": {"latency": 15, "priority": 1}
        }