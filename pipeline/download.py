import zipfile
from pathlib import Path
from typing import Optional

import requests
import typer
from cloudpathlib.gs import GSPath
from requests.exceptions import HTTPError
from tqdm import tqdm
from typing_extensions import Annotated

import pipeline.settings
from pipeline.settings import logger
from scipeds import constants

app = typer.Typer()

# IPEDS Completions files
BASE_URL = "https://nces.ed.gov/ipeds/datacenter/data"
COMPLETION_ZIP_FILENAMES = {
    year: f"{BASE_URL}/C{year}_A.zip" for year in range(constants.END_YEAR, 1999, -1)
}
COMPLETION_ZIP_FILENAMES.update(
    {
        year: f"{BASE_URL}/C{str(year - 1)[-2:]}{str(year)[-2:]}_A.zip"
        for year in range(1999, 1994, -1)
    }
)

COMPLETION_ZIP_FILENAMES.update(
    {year: f"{BASE_URL}/C{year}_CIP.zip" for year in range(1994, 1983, -1)}
)

COMPLETION_ZIP_FILENAMES[1990] = f"{BASE_URL}/C8990CIP.zip"

# Institution metadata files
INSTITUTION_METADATA_FILENAMES = {
    year: f"{BASE_URL}/HD{year}.zip" for year in range(constants.END_YEAR, 2010, -1)
}
INSTITUTION_METADATA_DATADICTS = {
    year: f"{BASE_URL}/HD{year}_Dict.zip" for year in range(constants.END_YEAR, 2010, -1)
}

# CIP Code Crosswalk files
CROSSWALK_ZIP_FILENAMES = {
    (2010, 2019): "https://nces.ed.gov/ipeds/cipcode/Files/Crosswalk2010to2020.csv",
    (2000, 2009): "https://nces.ed.gov/ipeds/cipcode/Files/Crosswalk2000to2010.csv",
    (1985, 1999): [
        "http://nces.ed.gov/pubs2002/cip2000/xls/cip.zip",
        "https://nces.ed.gov/pubs91/91396.pdf",
    ],
}

# Fall enrollment residence files
ENROLLMENT_RESIDENCE_FILENAMES = {
    year: f"{BASE_URL}/EF{year}C.zip" for year in range(constants.END_YEAR, 2000, -1)
}
ENROLLMENT_RESIDENCE_FILENAMES.update(
    {year: f"{BASE_URL}/EF{str(year)[-2:]}_C.zip" for year in [1998, 1996]}
)
ENROLLMENT_RESIDENCE_FILENAMES.update(
    {year: f"{BASE_URL}/EF{year}_C.zip" for year in [1986, 1988, 1992, 1994]}
)


# Directories
CROSSWALK_DIR = pipeline.settings.RAW_DATA_DIR / pipeline.settings.CROSSWALKS_DIRNAME
INSTITUTION_CHARACTERISTICS_DIR = pipeline.settings.RAW_DATA_DIR / constants.INSTITUTIONS_TABLE
COMPLETIONS_DIR = pipeline.settings.RAW_DATA_DIR / constants.COMPLETIONS_TABLE
ENROLLMENT_RESIDENCE_DIR = pipeline.settings.RAW_DATA_DIR / constants.ENROLLMENT_RESIDENCE_TABLE


def fetch_file(url: str, output_path: Path, verbose: bool = True):
    """Download file from url to output_path"""
    if verbose:
        logger.info(f"Fetching {url}...")
    r = requests.get(url, allow_redirects=True)
    try:
        r.raise_for_status()
    except HTTPError as e:
        logger.error(f"Failed to download file from {url}, received error code {r.status_code}.")
        raise e
    with open(output_path, "wb") as f:
        f.write(r.content)


def download_and_extract(url: str, output_dir: Path, verbose: bool = True):
    """Download the appropriate crosswalk file for a given year range"""
    filename = url.split("/")[-1]
    output_path = output_dir / filename

    # Use already-downloaded file if it exists
    if output_path.exists():
        if verbose:
            logger.info(f"File {output_path} exists, skipping download step...")
    else:
        fetch_file(url, output_path, verbose=verbose)

    if filename.endswith(".zip"):
        with zipfile.ZipFile(output_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)


@app.command()
def download_from_ipeds(
    survey_output_dir: Annotated[
        Path, typer.Option(help="Output directory for downloaded IPEDS-C surveys")
    ] = COMPLETIONS_DIR,
    crosswalk_output_dir: Annotated[
        Path, typer.Option(help="Output directory for CIP crosswalks")
    ] = CROSSWALK_DIR,
    institution_output_dir: Annotated[
        Path, typer.Option(help="Output directory for institution directory information")
    ] = INSTITUTION_CHARACTERISTICS_DIR,
    enrollment_residence_output_dir: Annotated[
        Path, typer.Option(help="Output directory for Fall Enrollment residence surveys")
    ] = ENROLLMENT_RESIDENCE_DIR,
    survey_year: Annotated[
        Optional[int],
        typer.Option(
            min=constants.START_YEAR,
            max=constants.END_YEAR,
            help=f"Select one survey year to download "
            f"(default: download all from {constants.START_YEAR}-{constants.END_YEAR})",
        ),
    ] = None,
    verbose: Annotated[bool, typer.Option(help="Include logging output")] = True,
):
    """Download all raw data directly from IPEDS."""
    # Download a single year of IPEDS data if requested / specified
    if survey_year is not None:
        year_dir = survey_output_dir / str(survey_year)
        year_dir.mkdir(parents=True, exist_ok=True)

        # Completions data
        # Note: this doesn't download the data dictionary, just the data
        filename = COMPLETION_ZIP_FILENAMES[survey_year]
        download_and_extract(filename, year_dir, verbose)

        # Institution metadata
        year_dir = institution_output_dir / str(survey_year)
        year_dir.mkdir(parents=True, exist_ok=True)
        filename = INSTITUTION_METADATA_FILENAMES[survey_year]
        download_and_extract(filename, year_dir, verbose)
        filename = INSTITUTION_METADATA_DATADICTS[survey_year]
        download_and_extract(filename, year_dir, verbose)

        # Fall enrollment residence info
        year_dir = enrollment_residence_output_dir / str(survey_year)
        year_dir.mkdir(parents=True, exist_ok=True)
        filename = ENROLLMENT_RESIDENCE_FILENAMES[survey_year]
        download_and_extract(filename, year_dir, verbose)

        return 0

    # Download completions data
    for year, filename in tqdm(COMPLETION_ZIP_FILENAMES.items()):
        year_dir = survey_output_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        download_and_extract(filename, year_dir, verbose)
        # Also download data dictionary
        dict_filename = filename.replace(".zip", "_Dict.zip")
        download_and_extract(dict_filename, year_dir, verbose)

    # Download crosswalk data
    for year_range, urls in tqdm(CROSSWALK_ZIP_FILENAMES.items()):
        start_year, end_year = year_range
        range_name = "-".join([str(start_year), str(end_year)])
        range_dir = crosswalk_output_dir / range_name
        range_dir.mkdir(parents=True, exist_ok=True)
        if isinstance(urls, str):
            urls = [urls]
        for url in urls:
            download_and_extract(url, range_dir, verbose)

    # Download institution metadata
    for year, filename in tqdm(INSTITUTION_METADATA_FILENAMES.items()):
        year_dir = institution_output_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        download_and_extract(filename, year_dir, verbose)

    # Download institution metadata data dictionaries
    for year, filename in tqdm(INSTITUTION_METADATA_DATADICTS.items()):
        year_dir = institution_output_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        download_and_extract(filename, year_dir, verbose)

    # Download fall enrollment residence data
    for year, filename in tqdm(ENROLLMENT_RESIDENCE_FILENAMES.items()):
        year_dir = enrollment_residence_output_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        download_and_extract(filename, year_dir, verbose)


@app.command()
def download_from_bucket(
    data_dir: Annotated[
        Path, typer.Option(help="Output directory for all raw data")
    ] = pipeline.settings.RAW_DATA_DIR,
):
    """Download raw data files directly from GC Storage"""
    raw_data_dir = GSPath(f"gs://{constants.SCIPEDS_BUCKET}/raw/")
    logger.info(f"Downloading raw IPEDS data to {data_dir}")
    raw_data_dir.download_to(data_dir)
    logger.success(f"Downloaded files to {data_dir}!")


if __name__ == "__main__":
    app()
