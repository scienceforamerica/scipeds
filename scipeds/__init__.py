from importlib.metadata import version

__version__: str = version("scipeds")

from scipeds.cli import download_db  # noqa:F401
