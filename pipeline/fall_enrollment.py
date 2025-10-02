from pathlib import Path

import pandas as pd
import typer
from tqdm import tqdm

import pipeline.settings
from pipeline.settings import logger
from scipeds import constants
from scipeds.utils import clean_name

ENROLLMENT_RESIDENCE_CODES = {
    'line': 'state_of_residence',
    'efcstate': 'state_of_residence',
    'efres01': 'all_first_time_students',
    'efres02': 'first_time_students_recently_graduated_high_school',
    
    ## These columns are not in recent data, so drop them
    # 'efres03': 'transfers',
    # 'efres04': 'first_professionals',
    # 'efres05': 'graduate_level_students',
    # 'centers': 'location_of_out_of_state_centers',
    # 'out_stat': 'location_of_out_of_state_centers',
}

FIPS_STATE_MAP = {
    "1": "AL",  # Alabama
    "2": "AK",  # Alaska
    "4": "AZ",  # Arizona
    "5": "AR",  # Arkansas
    "6": "CA",  # California
    "8": "CO",  # Colorado
    "9": "CT",  # Connecticut
    "01": "AL",  # Alabama
    "02": "AK",  # Alaska
    "04": "AZ",  # Arizona
    "05": "AR",  # Arkansas
    "06": "CA",  # California
    "08": "CO",  # Colorado
    "09": "CT",  # Connecticut
    "10": "DE",  # Delaware
    "11": "DC",  # District of Columbia
    "12": "FL",  # Florida
    "13": "GA",  # Georgia
    "15": "HI",  # Hawaii
    "16": "ID",  # Idaho
    "17": "IL",  # Illinois
    "18": "IN",  # Indiana
    "19": "IA",  # Iowa
    "20": "KS",  # Kansas
    "21": "KY",  # Kentucky
    "22": "LA",  # Louisiana
    "23": "ME",  # Maine
    "24": "MD",  # Maryland
    "25": "MA",  # Massachusetts
    "26": "MI",  # Michigan
    "27": "MN",  # Minnesota
    "28": "MS",  # Mississippi
    "29": "MO",  # Missouri
    "30": "MT",  # Montana
    "31": "NE",  # Nebraska
    "32": "NV",  # Nevada
    "33": "NH",  # New Hampshire
    "34": "NJ",  # New Jersey
    "35": "NM",  # New Mexico
    "36": "NY",  # New York
    "37": "NC",  # North Carolina
    "38": "ND",  # North Dakota
    "39": "OH",  # Ohio
    "40": "OK",  # Oklahoma
    "41": "OR",  # Oregon
    "42": "PA",  # Pennsylvania
    "44": "RI",  # Rhode Island
    "45": "SC",  # South Carolina
    "46": "SD",  # South Dakota
    "47": "TN",  # Tennessee
    "48": "TX",  # Texas
    "49": "UT",  # Utah
    "50": "VT",  # Vermont
    "51": "VA",  # Virginia
    "53": "WA",  # Washington
    "54": "WV",  # West Virginia
    "55": "WI",  # Wisconsin
    "56": "WY",  # Wyoming
    
    # Territories
    "60": "AS",   # American Samoa
    "64": "FM",   # Federated States of Micronesia
    "66": "GU",   # Guam
    "59": "GU",   # Guam (in older data)
    "68": "MH",   # Marshall Islands
    "69": "MP",   # Northern Marianas
    "70": "PW",   # Palau
    "72": "PR",   # Puerto Rico
    "61": "PR",   # Puerto Rico (in older data)
    "78": "VI",   # Virgin Islands
    "63": "VI",   # Virgin Islands (in older data)
    "62": "TTPI", # Trust Territory of the Pacific Islands
    
    # Others    
    "90": "foreign_countries",   # Foreign Countries
    "58": "us_total", # US total
    "89": "outlying_areas_total", 
    "57": "unknown",  # Unknown
    "98": "balance_line",  # Balance line
    "99": "grand_total",  # Grand Total
    "65": "grand_total",  # Grand total, older data

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
        if 'line' in df.columns and 'efcstate' in df.columns:
            if verbose:
                logger.warning("'line' and 'efcstate' columns both found, using 'efcstate'")
            df = df.drop('line', axis=1)
            
        # Only keep columns of interest
        keep_columns = ['unitid'] + [col for col in df.columns if col in ENROLLMENT_RESIDENCE_CODES]

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
        df['state_of_residence'] = df['state_of_residence'].str.strip()
        df['state_of_residence'] = df['state_of_residence'].map(FIPS_STATE_MAP)#.fillna(df['state_of_residence'])
    
        return df

    def read_year(
        self, folder: Path, verbose: bool = True
    ) -> pd.DataFrame:
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
        df.to_csv(output_file, compression="gzip")
        if verbose:
            logger.info(f"Wrote results for {year_dir.name} to {output_file}")

if __name__ == "__main__":
    typer.run(fall_enrollment_residence)
