from idmo_system.communication.api import ResourceFacingService


def _normalize_expression(expression: str) -> str:
    return " ".join(expression.strip().lower().split())


MOCK_TRANSLATIONS = {
    _normalize_expression(
        "We need a RAN with 100MHz bandwidth and 10ms latency. "
        "The core network should have 15ms latency and priority 1."
    ): {
        "ran": {"bandwidth_mhz": 100, "latency_ms": 10},
        "core": {"latency_ms": 15, "priority": 1},
    },
    _normalize_expression(
        "Set up a slice for 20 seconds with core priority 2 and 50ms latency. RAN bandwidth must be 20 MHz in TA 1000."
    ): {
        "ran": {"bandwidth_mhz": 20, "tac": 1000, "duration_s": 20},
        "core": {"latency_ms": 50, "priority": 2},
    },
}


async def _mock_translate_to_rfs(self, expression: str) -> dict | None:
    translation = MOCK_TRANSLATIONS.get(_normalize_expression(expression))
    if translation is None:
        return None
    return ResourceFacingService(**translation).model_dump()


def install():
    """Replace the LLM call in IntentTranslator with a lookup in MOCK_TRANSLATIONS."""
    from idmo_system.gateway.translator import IntentTranslator
    IntentTranslator.translate_to_rfs = _mock_translate_to_rfs
