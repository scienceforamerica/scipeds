import os
from pathlib import Path

from platformdirs import user_cache_path

from scipeds import __version__

# DB attributes
VERSION_STR = __version__.replace(".", "_")  # type: ignore[has-type]
DB_NAME = f"scipeds_{VERSION_STR}.duckdb"
COMPLETIONS_TABLE = "ipeds_completions_a"
INSTITUTIONS_TABLE = "ipeds_directory_info"
CIP_TABLE = "cip_info"

# Start and end year for analysis
START_YEAR = 1984
END_YEAR = 2024

# Storage bucket
SCIPEDS_BUCKET = "scipeds-data"

# Cache dir
SCIPEDS_CACHE_DIR = Path(
    os.environ.get("SCIPEDS_CACHE_DIR", (user_cache_path() / ".scipeds").resolve())
)
