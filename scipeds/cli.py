from pathlib import Path
from shutil import rmtree

import typer
from cloudpathlib.exceptions import CloudPathNotExistsError
from cloudpathlib.gs import GSPath

from scipeds.constants import DB_NAME, SCIPEDS_BUCKET, SCIPEDS_CACHE_DIR

app = typer.Typer()


@app.command()
def download_db(
    output_path: Path = SCIPEDS_CACHE_DIR / DB_NAME,
    overwrite: bool = False,
    verbose: bool = True,
):
    """Downloads a pre-processed duckdb file to the local machine.

    Args:
        output_path (Path, optional): Path to save db.
            Defaults to `.scipeds/ipeds.duckdb` in the user's cache path.
        overwrite (bool, optional): Whether to re-download and
            overwrite the cached db if it already exists.
            Defaults to False.
        verbose (bool, optional): Whether to verbosely log. Defaults to True.
    """
    if output_path is None:
        if verbose:
            print(
                f"No download path specified, defaulting to user cache directory "
                f"{SCIPEDS_CACHE_DIR}."
            )
        output_path = SCIPEDS_CACHE_DIR / DB_NAME
    if not (parent := output_path.parents[0]).exists():
        if verbose:
            print(f"Creating scipeds cache directory {parent.resolve()}.")
        parent.mkdir(parents=True)
    if output_path.exists() and not overwrite:
        print(
            f"Database already downloaded to {output_path}. To re-download and "
            "overwrite the existing file, re-run with `overwrite` set to `True`"
            "(from within Python) or add --overwrite to your CLI command."
        )
        return

    processed_db = GSPath(f"gs://{SCIPEDS_BUCKET}/processed/{DB_NAME}")

    if verbose:
        print(f"Downloading pre-processed IPEDS db to {output_path}")

    try:
        processed_db.download_to(output_path)
    except CloudPathNotExistsError:
        raise CloudPathNotExistsError(
            f"{processed_db} not found by scipeds. If you think it should exist, "
            "please file an issue."
        )

    if verbose:
        print("Download complete.")
        show_cache_usage(verbose)


@app.command()
def show_cache_usage(verbose: bool = True) -> float:
    """Reports how much storage is being used by scipeds"""
    sizes = [f.stat().st_size for f in SCIPEDS_CACHE_DIR.glob("**/*") if f.is_file()]
    usage_mb = sum(sizes) // 1e6
    count = len(sizes)
    if verbose:
        print(f"scipeds has {count} files and is using {usage_mb:,.0f}MB in {SCIPEDS_CACHE_DIR}")
    return usage_mb


@app.command()
def clean_cache(verbose: bool = True):
    """Removes cached db file if it exists"""
    if not SCIPEDS_CACHE_DIR.exists():
        print(f"Cache directory {SCIPEDS_CACHE_DIR} does not exist.")
        return 0
    if verbose:
        print(f"Cleaning cache directory {SCIPEDS_CACHE_DIR}")

    show_cache_usage(verbose=True)
    typer.confirm("Are you sure you want to delete the cache?", abort=True)

    rmtree(SCIPEDS_CACHE_DIR)

    if verbose:
        print(f"Cleaned cache directory {SCIPEDS_CACHE_DIR}")


if __name__ == "__main__":
    app()
