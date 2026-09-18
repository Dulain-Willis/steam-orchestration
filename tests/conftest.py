"""Airflow reads AIRFLOW_HOME at import time, so this must run before
anything imports airflow — hence setting it here, at conftest module load,
rather than in a fixture.
"""

import os
import subprocess
import sys
from pathlib import Path

_AIRFLOW_HOME = Path(__file__).parent / ".airflow_home_test"
os.environ.setdefault("AIRFLOW_HOME", str(_AIRFLOW_HOME))

_AIRFLOW_HOME.mkdir(exist_ok=True)
if not (_AIRFLOW_HOME / "airflow.db").exists():
    subprocess.run(
        [sys.executable, "-m", "airflow", "db", "migrate"],
        check=True,
        capture_output=True,
        env=os.environ | {"AIRFLOW_HOME": str(_AIRFLOW_HOME)},
    )
