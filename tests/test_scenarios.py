"""Runs every scenario in scenarios/ (mock transport) and checks its `expect` section."""

import asyncio
from pathlib import Path

import pytest

from idmo_system.runner import ScenarioRunner
from idmo_system.scenario import Scenario

SCENARIO_FILES = sorted((Path(__file__).parent.parent / "scenarios").glob("*.yaml"))


@pytest.mark.scenario
@pytest.mark.parametrize("path", SCENARIO_FILES, ids=lambda p: p.stem)
def test_scenario(path, tmp_path):
    scenario = Scenario.from_yaml(path)
    assert scenario.expect, f"{path.name} has no `expect` section"
    assert scenario.runtime.transport == "mock" and scenario.runtime.llm == "mock"
    scenario.runtime.output_dir = str(tmp_path)

    result = asyncio.run(ScenarioRunner(scenario).run())

    assert result.passed, "\n" + result.summary()
    assert (tmp_path / scenario.name / "report.json").exists()
