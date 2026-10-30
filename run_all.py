"""Runs the basic demo scenario. Equivalent to:

    python -m idmo_system run scenarios/basic.yaml

See scenarios/ for more examples and idmo_system/scenario.py for all options.
"""

import asyncio
import sys

from loguru import logger

from idmo_system.runner import ScenarioRunner
from idmo_system.scenario import Scenario

SCENARIO_FILE = "scenarios/multi_domain.yaml"


if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="INFO")

    result = asyncio.run(ScenarioRunner(Scenario.from_yaml(SCENARIO_FILE)).run())
    print()
    print(result.summary())
