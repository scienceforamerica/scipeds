import os
from pathlib import Path

from platformdirs import user_cache_path

# DB attributes
DB_NAME = "ipeds.duckdb"
COMPLETIONS_TABLE = "ipeds_completions_a"
INSTITUTIONS_TABLE = "ipeds_directory_info"
CIP_TABLE = "cip_info"

# Start and end year for analysis
START_YEAR = 1995
END_YEAR = 2023

# Storage bucket
SCIPEDS_BUCKET = "scipeds-data"

# Cache dir
SCIPEDS_CACHE_DIR = Path(
    os.environ.get("SCIPEDS_CACHE_DIR", (user_cache_path() / ".scipeds").resolve())
)
