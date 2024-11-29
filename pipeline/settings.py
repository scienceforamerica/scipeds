import os
from pathlib import Path

from loguru import logger
from tqdm import tqdm

# Make loguru inter-operable with tqdm
logger.remove()
logger.add(lambda msg: tqdm.write(msg, end=""), colorize=True)

PROJ_ROOT = Path(__file__).resolve().parents[1]
LIBRARY_ROOT = PROJ_ROOT / "scipeds"

# Data folder structure
DATA_PATH = Path(os.environ.get("DATA_PATH", PROJ_ROOT / "data"))
RAW_DATA_DIR = DATA_PATH / "raw"
INTERIM_DATA_DIR = DATA_PATH / "interim"
PROCESSED_DATA_DIR = DATA_PATH / "processed"
CROSSWALKS_DIRNAME = "ipeds_crosswalks"
