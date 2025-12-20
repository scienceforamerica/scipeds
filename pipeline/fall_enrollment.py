from pathlib import Path

import numpy as np
import pandas as pd
import typer
from tqdm import tqdm

import pipeline.settings
from pipeline.settings import logger
from scipeds import constants
from scipeds.utils import clean_name
from scipeds.data.enums import Geo

ENROLLMENT_RESIDENCE_CODES = {
    "line": "state_of_residence",
    "efcstate": "state_of_residence",
    "efres01": "all_first_time_students",
    "efres02": "first_time_students_recently_graduated_high_school",
    ## These columns are not in recent data, so drop them
    # 'efres03': 'transfers',
    # 'efres04': 'first_professionals',
    # 'efres05': 'graduate_level_students',
    # 'centers': 'location_of_out_of_state_centers',
    # 'out_stat': 'location_of_out_of_state_centers',
}

FIPS_STATE_MAP = {
    "1": Geo.AL,
    "2": Geo.AK,
    "4": Geo.AZ,
    "5": Geo.AR,
    "6": Geo.CA,
    "8": Geo.CO,
    "9": Geo.CT,
    "01": Geo.AL,
    "02": Geo.AK,
    "04": Geo.AZ,
    "05": Geo.AR,
    "06": Geo.CA,
    "08": Geo.CO,
    "09": Geo.CT,
    "10": Geo.DE,
    "11": Geo.DC,
    "12": Geo.FL,
    "13": Geo.GA,
    "15": Geo.HI,
    "16": Geo.ID,
    "17": Geo.IL,
    "18": Geo.IN,
    "19": Geo.IA,
    "20": Geo.KS,
    "21": Geo.KY,
    "22": Geo.LA,
    "23": Geo.ME,
    "24": Geo.MD,
    "25": Geo.MA,
    "26": Geo.MI,
    "27": Geo.MN,
    "28": Geo.MS,
    "29": Geo.MO,
    "30": Geo.MT,
    "31": Geo.NE,
    "32": Geo.NV,
    "33": Geo.NH,
    "34": Geo.NJ,
    "35": Geo.NM,
    "36": Geo.NY,
    "37": Geo.NC,
    "38": Geo.ND,
    "39": Geo.OH,
    "40": Geo.OK,
    "41": Geo.OR,
    "42": Geo.PA,
    "44": Geo.RI,
    "45": Geo.SC,
    "46": Geo.SD,
    "47": Geo.TN,
    "48": Geo.TX,
    "49": Geo.UT,
    "50": Geo.VT,
    "51": Geo.VA,
    "53": Geo.WA,
    "54": Geo.WV,
    "55": Geo.WI,
    "56": Geo.WY,
    # Territories
    "60": Geo.AS,
    "64": Geo.FM,
    "66": Geo.GU,
    "59": Geo.GU,
    "68": Geo.MH,
    "69": Geo.MP,
    "70": Geo.PW,
    "72": Geo.PR,
    "61": Geo.PR,
    "78": Geo.VI,
    "63": Geo.VI,
    "62": Geo.TTPI,
    # Others
    "90": Geo.foreign_countries,
    "58": Geo.us_total,
    "89": Geo.outlying_areas_total,
    "57": Geo.unknown,
    "98": Geo.balance_line,
    "99": Geo.grand_total,
    "65": Geo.grand_total,
}

assert len(FIPS_STATE_MAP.keys()) == len(set(FIPS_STATE_MAP.keys()))


class IPEDSFallEnrollmentReader:
    """Class for reading, cleaning, tidying, and transforming historical IPEDS
    Fall Enrollment data.
    """

    def __init__(self):
        pass

    def _read_raw_data_files(self, folder: Path, verbose: bool = True) -> pd.DataFrame:
        """Find and read the raw data file containing residence information"""
        # Load raw data, using revised data if it exists
        csv_files = list(folder.glob("*rv.csv"))
        if len(csv_files) == 0:
            if verbose:
                logger.info("Revised data not found, attempting to use raw data.")
            csv_files = list(folder.glob("*.csv"))
            if len(csv_files) == 0:
                raise FileNotFoundError(f"Could not find any raw data CSVs in {folder}")
        raw_file = csv_files[0]
        if not raw_file.exists():
            raise FileNotFoundError(f"Raw CSV data file not found in {folder}")
        raw_df = pd.read_csv(raw_file, dtype=str)
        if verbose:
            logger.info(
                f"Read raw data with {raw_df.shape[0]:,} rows and {raw_df.shape[1]:,} cols"
            )
        return raw_df

    def _clean(self, df: pd.DataFrame, verbose: bool = True) -> pd.DataFrame:
        """Clean up the dataframe"""
        # Tidy column names
        df.rename(columns={col: clean_name(col) for col in df.columns}, inplace=True)

        # If both 'line' and 'efcstate' columns exists, drop 'line'
        if "line" in df.columns and "efcstate" in df.columns:
            if verbose:
                logger.warning("'line' and 'efcstate' columns both found, using 'efcstate'")
            df = df.drop("line", axis=1)

        # Only keep columns of interest
        keep_columns = ["unitid"] + [
            col for col in df.columns if col in ENROLLMENT_RESIDENCE_CODES
        ]

        return df[keep_columns]

    def _translate_transform(
        self, df: pd.DataFrame, year: int, verbose: bool = True
    ) -> pd.DataFrame:
        """Translate columns using data dictionaries and transform into a tidy dataframe"""

        # Add year column
        df["year"] = year

        # Translate column codes
        df = df.rename(columns=ENROLLMENT_RESIDENCE_CODES)

        # Translate states to abbreviations
        df["state_of_residence"] = df["state_of_residence"].str.strip()
        df["state_of_residence"] = df["state_of_residence"].map(FIPS_STATE_MAP)

        # Replace periods with NaN
        df["first_time_students_recently_graduated_high_school"] = df[
            "first_time_students_recently_graduated_high_school"
        ].replace(".", np.nan)

        # Return columns always in the same order
        col_order = [
            "unitid",
            "year",
            "state_of_residence",
            "all_first_time_students",
            "first_time_students_recently_graduated_high_school",
        ]

        return df[col_order]

    def read_year(self, folder: Path, verbose: bool = True) -> pd.DataFrame:
        try:
            year = int(folder.name)
        except Exception:
            raise Exception(f"Provided path {folder} could not be interpreted as a year.")
        if verbose:
            logger.info(f"Reading fall enrollment residence data for the year {year} in {folder}")
        df = self._read_raw_data_files(folder, verbose=verbose)
        df = self._clean(df, verbose=verbose)
        df = self._translate_transform(df, year=year, verbose=verbose)

        if verbose:
            logger.info(f"Returning df with {df.shape[0]:,} rows and {df.shape[1]:,} cols")
        return df


def fall_enrollment_residence(
    survey_dir: Path = pipeline.settings.RAW_DATA_DIR / constants.ENROLLMENT_RESIDENCE_TABLE,
    output_dir: Path = pipeline.settings.INTERIM_DATA_DIR / constants.ENROLLMENT_RESIDENCE_TABLE,
    verbose: bool = True,
):
    """Process raw data into interim CSV files"""
    reader = IPEDSFallEnrollmentReader()
    year_dirs = sorted([d for d in survey_dir.iterdir() if d.is_dir()], reverse=False)
    for year_dir in tqdm(year_dirs):
        df = reader.read_year(year_dir, verbose=verbose)
        csv_dir = output_dir
        csv_dir.mkdir(parents=True, exist_ok=True)
        output_file = csv_dir / (year_dir.name + ".csv.gz")
        df.to_csv(output_file, compression="gzip", index=False)
        if verbose:
            logger.info(f"Wrote results for {year_dir.name} to {output_file}")


if __name__ == "__main__":
    typer.run(fall_enrollment_residence)
