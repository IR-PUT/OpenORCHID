from idmo_system.gateway import translator as translator_module


def _normalize_expression(expression: str) -> str:
    return " ".join(expression.strip().lower().split())


MOCK_TRANSLATIONS = {
    _normalize_expression(
        "We need a RAN with 100MHz bandwidth and 10ms latency. "
        "The core network should have 15ms latency and priority 1."
    ): {
        "ran": {"bandwidth": 100, "latency": 10},
        "core": {"latency": 15, "priority": 1},
    },
    _normalize_expression(
        "Set up a slice with core priority 2 and 50ms latency. RAN bandwidth must be 20."
    ): {
        "ran": {"bandwidth": 20, "latency": None},
        "core": {"latency": 50, "priority": 2},
    },
}


_original_translate_to_rfs = translator_module.IntentTranslator.translate_to_rfs


async def _mock_translate_to_rfs(self, expression: str) -> dict:
    normalized = _normalize_expression(expression)

    if normalized in MOCK_TRANSLATIONS:
        return MOCK_TRANSLATIONS[normalized]
    
    return None

    # return await _original_translate_to_rfs(self, expression)


translator_module.IntentTranslator.translate_to_rfs = _mock_translate_to_rfs
