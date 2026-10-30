import json

from idmo_system.communication.api import ResourceFacingService
from idmo_system.gateway.translator import EXAMPLES, IntentTranslator
from idmo_system.gateway.translator_eval import compare


def test_prompt_examples_follow_schema_field_order():
    # The LLM output is constrained to the JSON schema; examples in a different field order made the small
    # model answer null for the field it "expected" later (core.priority was always lost).
    schema = ResourceFacingService.model_json_schema()
    prompt = IntentTranslator()._prompt
    for _, rfs in EXAMPLES:
        example = json.loads(rfs.model_dump_json())
        assert rfs.model_dump_json() in prompt
        for part, model in (("ran", "RanRFS"), ("core", "CoreRFS")):
            assert list(example[part]) == list(schema["$defs"][model]["properties"])


def test_compare_counts_fields_and_flags_invented_values():
    expected = {"ran": {"bandwidth_mhz": 100, "tac": 1000}, "core": {}}
    actual = {"ran": {"bandwidth_mhz": 100.0000000001, "tac": None, "latency_ms": 5}, "core": {"priority": None}}

    correct, total, problems = compare(expected, actual)

    assert (correct, total) == (1, 2)
    assert problems == ["ran.tac: expected 1000, got None", "ran.latency_ms: unexpected 5"]


def test_compare_failed_translation():
    assert compare({"ran": {"tac": 1}}, None) == (0, 1, ["ran.tac: expected 1, got None"])
