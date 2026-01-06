import os
from dotenv import load_dotenv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent  # .. from health_dbt
load_dotenv(ROOT / ".env")

print("Using analytics schema:", os.environ.get("SNOWFLAKE_ANALYTICS_SCHEMA"))
# Call dbt with the same arguments you pass to Python
cmd = ["dbt"] + sys.argv[1:]
result = subprocess.run(cmd, check=False)
sys.exit(result.returncode)
