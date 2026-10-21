"""Command line interface.

    python -m idmo_system validate scenarios/basic.yaml
    python -m idmo_system run scenarios/multi_domain.yaml [--log-level DEBUG]
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
    print(f"  Transport:    {scenario.runtime.transport}, LLM: {scenario.runtime.llm}")
    return 0


def cmd_run(args) -> int:
    scenario = _load(args.scenario)
    log.remove()
    log.add(sys.stderr, level=args.log_level)

    if args.output_dir is not None:
        scenario.runtime.output_dir = args.output_dir

    from idmo_system.runner import ScenarioRunner
    result = asyncio.run(ScenarioRunner(scenario).run())
    print()
    print(result.summary())
    # Non-zero exit code when an `expect` entry failed or a domain did not register - usable in CI.
    return 0 if result.passed else 1


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
    p_run.set_defaults(func=cmd_run)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
