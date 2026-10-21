"""Command line interface.

    python -m idmo_system validate scenarios/basic.yaml
    python -m idmo_system run scenarios/multi_domain.yaml [--log-level DEBUG]
    python -m idmo_system evaluate-llm [--model qwen2.5-coder:1.5b]
"""

import argparse
import asyncio
import sys

from loguru import logger as log
from pydantic import ValidationError

from idmo_system.scenario import Scenario


def _load(path: str) -> Scenario:
    try:
        return Scenario.from_yaml(path)
    except FileNotFoundError:
        sys.exit(f"Scenario file not found: {path}")
    except ValidationError as e:
        sys.exit(f"Invalid scenario {path}:\n{e}")


def cmd_validate(args) -> int:
    scenario = _load(args.scenario)
    ran = [d.name for d in scenario.domains if d.type == "ran"]
    core = [d.name for d in scenario.domains if d.type == "core"]
    print(f"Scenario '{scenario.name}' is valid.")
    print(f"  RAN domains:  {', '.join(ran) or '-'}")
    print(f"  CORE domains: {', '.join(core) or '-'}")
    print(f"  Intents:      {len(scenario.intents)}")
    print(f"  Events:       {len(scenario.events)}")
    print(f"  Expectations: {len(scenario.expect)}")
    print(f"  Kafka: {scenario.runtime.kafka}, NATS: {scenario.runtime.nats}, LLM: {scenario.runtime.llm}")
    return 0


def cmd_run(args) -> int:
    scenario = _load(args.scenario)
    log.remove()
    log.add(sys.stderr, level=args.log_level)

    for option in ("output_dir", "kafka", "nats", "llm"):
        if getattr(args, option) is not None:
            setattr(scenario.runtime, option, getattr(args, option))

    from idmo_system.runner import ScenarioRunner
    result = asyncio.run(ScenarioRunner(scenario).run())
    print()
    print(result.summary())
    # Non-zero exit code when an `expect` entry failed or a domain did not register - usable in CI.
    return 0 if result.passed else 1


def cmd_evaluate_llm(args) -> int:
    from idmo_system.gateway.translator import IntentTranslator
    from idmo_system.gateway.translator_eval import evaluate

    log.remove()
    log.add(sys.stderr, level="ERROR")
    print(f"Evaluating {args.model} at {args.llm_address} ...")
    print(asyncio.run(evaluate(IntentTranslator(llm_address=args.llm_address, model=args.model))))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m idmo_system", description="OpenORCHID scenario runner")
    sub = parser.add_subparsers(dest="command", required=True)

    p_validate = sub.add_parser("validate", help="Validate a scenario file")
    p_validate.add_argument("scenario")
    p_validate.set_defaults(func=cmd_validate)

    p_run = sub.add_parser("run", help="Run a scenario")
    p_run.add_argument("scenario")
    p_run.add_argument("--log-level", default="INFO", help="Console log level (default: INFO)")
    p_run.add_argument("--output-dir", help="Override runtime.output_dir (logs and report.json)")
    p_run.add_argument("--kafka", choices=["mock", "real"], help="Override runtime.kafka")
    p_run.add_argument("--nats", choices=["mock", "real"], help="Override runtime.nats")
    p_run.add_argument("--llm", choices=["mock", "ollama"], help="Override runtime.llm")
    p_run.set_defaults(func=cmd_run)

    p_eval = sub.add_parser("evaluate-llm", help="Measure the accuracy of the LLM intent translation (needs Ollama)")
    p_eval.add_argument("--llm-address", default="http://127.0.0.1:11434/api/generate")
    p_eval.add_argument("--model", default="qwen2.5-coder:1.5b")
    p_eval.set_defaults(func=cmd_evaluate_llm)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
