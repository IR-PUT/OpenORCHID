"""Runs every scenario against real infrastructure from docker-compose.yaml.

Skipped unless OPENORCHID_REAL lists the services to use, e.g.:

    docker compose up -d broker nats
    OPENORCHID_REAL=kafka,nats uv run pytest -m real_infra

Each scenario runs in a separate process: the mocks installed by conftest.py patch sys.modules of this process.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
SCENARIO_FILES = sorted((ROOT / "scenarios").glob("*.yaml"))
REAL_SERVICES = [s.strip() for s in os.environ.get("OPENORCHID_REAL", "").split(",") if s.strip()]


@pytest.mark.real_infra
@pytest.mark.skipif(not REAL_SERVICES, reason="set OPENORCHID_REAL=kafka,nats to run against docker-compose services")
@pytest.mark.parametrize("path", SCENARIO_FILES, ids=lambda p: p.stem)
def test_scenario_on_real_infrastructure(path, tmp_path):
    unknown = set(REAL_SERVICES) - {"kafka", "nats"}
    assert not unknown, f"OPENORCHID_REAL supports kafka and nats, got {sorted(unknown)}"

    command = [sys.executable, "-m", "idmo_system", "run", str(path), "--output-dir", str(tmp_path), "--log-level", "WARNING"]
    for service in REAL_SERVICES:
        command += [f"--{service}", "real"]

    completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=300)

    assert completed.returncode == 0, f"\n{completed.stdout}\n{completed.stderr[-3000:]}"
