"""Measures how well the LLM translates natural-language intents into RFS: field-level accuracy and latency.

    python -m idmo_system evaluate-llm [--llm-address URL] [--model NAME]

The cases include the two few-shot examples from the prompt (the model has seen them) and new phrasings.
"""

import time
from dataclasses import dataclass

from idmo_system.gateway.translator import IntentTranslator


@dataclass
class TranslationCase:
    label: str
    expression: str
    expected: dict # {"ran": {...}, "core": {...}} - only the fields that must be extracted, everything else null


CASES = [
    TranslationCase(
        "prompt example 1",
        "We need a RAN with 100MHz bandwidth and 10ms latency. The core network should have 15ms latency and priority 1.",
        {"ran": {"bandwidth_mhz": 100, "latency_ms": 10}, "core": {"latency_ms": 15, "priority": 1}}),
    TranslationCase(
        "prompt example 2",
        "Set up a slice for 20 seconds with core priority 2 and 50ms latency. RAN bandwidth must be 20 MHz in TA 1000.",
        {"ran": {"bandwidth_mhz": 20, "tac": 1000, "duration_s": 20}, "core": {"latency_ms": 50, "priority": 2}}),
    TranslationCase(
        "ran only",
        "Low-latency slice for robots in TA 1000 for 20 seconds, 100 MHz, latency under 10 ms.",
        {"ran": {"bandwidth_mhz": 100, "latency_ms": 10, "tac": 1000, "duration_s": 20}, "core": {}}),
    TranslationCase(
        "throughput and UEs",
        "Provide 300 Mbps throughput for 150 devices in tracking area 2000 for 30 seconds.",
        {"ran": {"throughput_mbps": 300, "num_ues": 150, "tac": 2000, "duration_s": 30}, "core": {}}),
    TranslationCase(
        "duration in minutes",
        "Create a slice with 40 MHz of bandwidth that lasts 2 minutes, core priority 3.",
        {"ran": {"bandwidth_mhz": 40, "duration_s": 120}, "core": {"priority": 3}}),
    TranslationCase(
        "core only",
        "The core network must keep latency below 8 ms with the highest priority 1.",
        {"ran": {}, "core": {"latency_ms": 8, "priority": 1}}),
    TranslationCase(
        "all fields",
        "Stadium event in TA 4000: 200 MHz, 500 users, at least 150 Mbps and 20 ms radio latency for 45 seconds; "
        "core priority 1 with 15 ms latency.",
        {"ran": {"bandwidth_mhz": 200, "num_ues": 500, "throughput_mbps": 150, "latency_ms": 20, "tac": 4000,
                 "duration_s": 45}, "core": {"priority": 1, "latency_ms": 15}}),
    TranslationCase(
        "informal",
        "need a quick slice, 60 megahertz, area 3000, 10 ms ran latency, keep it for half a minute",
        {"ran": {"bandwidth_mhz": 60, "tac": 3000, "latency_ms": 10, "duration_s": 30}, "core": {}}),
]


def compare(expected: dict, actual: dict | None) -> tuple[int, int, list[str]]:
    """Returns (correct fields, expected fields, problems); values the text does not contain count as problems."""
    correct, total, problems = 0, 0, []
    for part in ("ran", "core"):
        exp, act = expected.get(part, {}), (actual or {}).get(part) or {}
        for key, value in exp.items():
            total += 1
            if act.get(key) is not None and abs(float(act[key]) - float(value)) < 1e-6:
                correct += 1
            else:
                problems.append(f"{part}.{key}: expected {value}, got {act.get(key)}")
        for key, value in act.items():
            if key not in exp and value is not None:
                problems.append(f"{part}.{key}: unexpected {value}")
    return correct, total, problems


async def evaluate(translator: IntentTranslator, cases: list[TranslationCase] = CASES) -> str:
    lines = []
    all_correct = all_total = exact = 0
    for case in cases:
        start = time.monotonic()
        actual = await translator.translate_to_rfs(case.expression)
        elapsed = time.monotonic() - start
        correct, total, problems = compare(case.expected, actual)
        if actual is None:
            problems = ["translation failed"]
        all_correct += correct
        all_total += total
        exact += not problems
        status = "OK   " if not problems else "WRONG"
        lines.append(f"[{status}] {case.label:<20} {elapsed:5.1f}s  {correct}/{total} fields"
                     + (f"  -> {'; '.join(problems)}" if problems else ""))
    lines.append(f"\nFields: {all_correct}/{all_total} ({100 * all_correct / all_total:.0f}%), "
                 f"exact translations: {exact}/{len(cases)}")
    return "\n".join(lines)
