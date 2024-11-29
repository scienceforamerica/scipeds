from pathlib import Path

import pandas as pd
import typer
from tqdm import tqdm

import pipeline.settings
from pipeline.cip_crosswalk import CIPCodeCrosswalk, DHSClassifier, NCSESClassifier
from pipeline.settings import logger
from scipeds import constants
from scipeds.data.enums import AwardLevel, Gender, NCSESSciGroup, RaceEthn
from scipeds.utils import clean_name

PRE_2010_CRACE_CODES = {
    "crace01": (RaceEthn.nonres.value, Gender.men.value),
    "crace02": (RaceEthn.nonres.value, Gender.women.value),
    "crace03": (RaceEthn.black_or_aa.value, Gender.men.value),
    "crace04": (RaceEthn.black_or_aa.value, Gender.women.value),
    "crace05": (RaceEthn.american_indian.value, Gender.men.value),
    "crace06": (RaceEthn.american_indian.value, Gender.women.value),
    "crace07": (RaceEthn.asian.value, Gender.men.value),
    "crace08": (RaceEthn.asian.value, Gender.women.value),
    "crace09": (RaceEthn.hispanic.value, Gender.men.value),
    "crace10": (RaceEthn.hispanic.value, Gender.women.value),
    "crace11": (RaceEthn.white.value, Gender.men.value),
    "crace12": (RaceEthn.white.value, Gender.women.value),
    "crace13": (RaceEthn.unknown.value, Gender.men.value),
    "crace14": (RaceEthn.unknown.value, Gender.women.value),
}

INTERIM_CRACE_CODES = {
    "cunknm": (RaceEthn.unknown.value, Gender.men.value),
    "cunknw": (RaceEthn.unknown.value, Gender.women.value),
    "cnralm": (RaceEthn.nonres.value, Gender.men.value),
    "cnralw": (RaceEthn.nonres.value, Gender.women.value),
    "dvcaim": (RaceEthn.american_indian.value, Gender.men.value),
    "dvcaiw": (RaceEthn.american_indian.value, Gender.women.value),
    "dvcapm": (RaceEthn.asian.value, Gender.men.value),
    "dvcapw": (RaceEthn.asian.value, Gender.women.value),
    "dvcbkm": (RaceEthn.black_or_aa.value, Gender.men.value),
    "dvcbkw": (RaceEthn.black_or_aa.value, Gender.women.value),
    "dvchsm": (RaceEthn.hispanic.value, Gender.men.value),
    "dvchsw": (RaceEthn.hispanic.value, Gender.women.value),
    "dvcwhm": (RaceEthn.white.value, Gender.men.value),
    "dvcwhw": (RaceEthn.white.value, Gender.women.value),
}

POST_2010_CRACE_CODES = {
    "cunknm": (RaceEthn.unknown.value, Gender.men.value),
    "cunknw": (RaceEthn.unknown.value, Gender.women.value),
    "cnralm": (RaceEthn.nonres.value, Gender.men.value),
    "cnralw": (RaceEthn.nonres.value, Gender.women.value),
    "caianm": (RaceEthn.american_indian.value, Gender.men.value),
    "caianw": (RaceEthn.american_indian.value, Gender.women.value),
    "casiam": (RaceEthn.asian.value, Gender.men.value),
    "casiaw": (RaceEthn.asian.value, Gender.women.value),
    "cbkaam": (RaceEthn.black_or_aa.value, Gender.men.value),
    "cbkaaw": (RaceEthn.black_or_aa.value, Gender.women.value),
    "chispm": (RaceEthn.hispanic.value, Gender.men.value),
    "chispw": (RaceEthn.hispanic.value, Gender.women.value),
    "cnhpim": (RaceEthn.hawaiian_pi.value, Gender.men.value),
    "cnhpiw": (RaceEthn.hawaiian_pi.value, Gender.women.value),
    "cwhitm": (RaceEthn.white.value, Gender.men.value),
    "cwhitw": (RaceEthn.white.value, Gender.women.value),
    "c2morm": (RaceEthn.two_or_more.value, Gender.men.value),
    "c2morw": (RaceEthn.two_or_more.value, Gender.women.value),
}

# Based on https://nces.ed.gov/ipeds/report-your-data/data-tip-sheet-reporting-graduate-awards
# we're mapping all First-professional degrees (10) to Professional Doctorate
# and all First-professional certificates (11) to Post-master's Degree
AWARD_LEVEL_CODES = {
    -1: AwardLevel.unknown.value,
    1: AwardLevel.lt1.value,
    2: AwardLevel.gt1_lt2.value,
    3: AwardLevel.associates.value,
    4: AwardLevel.gt2_lt4.value,
    5: AwardLevel.bachelors.value,
    6: AwardLevel.postbac.value,
    7: AwardLevel.masters.value,
    8: AwardLevel.postmas.value,
    9: AwardLevel.doctor_research.value,
    10: AwardLevel.doctor_professional.value,
    11: AwardLevel.postmas.value,
    17: AwardLevel.doctor_research.value,
    18: AwardLevel.doctor_professional.value,
    19: AwardLevel.doctor_other.value,
    20: AwardLevel.lt_12w.value,
    21: AwardLevel.gt_12w_lt_1y.value,
}

INDEX_COLS = ["unitid", "cipcode", "awlevel", "majornum"]
ALL_CRACE_CODES = PRE_2010_CRACE_CODES | INTERIM_CRACE_CODES | POST_2010_CRACE_CODES


class IPEDSCompletionsReader:
    """Class for reading, cleaning, tidying, and transforming historical IPEDS completions data
    and to decorate with additional data like STEM classification"""

    def __init__(self):
        self.crosswalk = CIPCodeCrosswalk()
        self.ncses_classifier = NCSESClassifier()
        self.dhs_classifier = DHSClassifier()

    def _read_raw_data_files(self, folder: Path, verbose: bool = True) -> pd.DataFrame:
        """Find and read the raw data file from"""
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

        # Make sure a "majornum" column exists
        if "majornum" not in df.columns:
            # If it doesn't, assume all reported majors are first majors
            if verbose:
                logger.info("MAJORNUM column not found, assuming all majors are first majors.")
            df["majornum"] = 1
        else:
            df["majornum"] = df["majornum"].astype(int)

        # Clean cipcode column
        df["cipcode"] = df["cipcode"].str.strip()

        # Only keep columns of interest
        keep_columns = [col for col in df.columns if col in INDEX_COLS or col in ALL_CRACE_CODES]

        assert all(col in df.columns for col in INDEX_COLS)
        return df[keep_columns]

    def _translate_transform(
        self, df: pd.DataFrame, year: int, remove_zero: bool = True, verbose: bool = True
    ) -> pd.DataFrame:
        """Translate columns using data dictionaries and transform into a tidy dataframe"""
        # Add year column
        df["year"] = year

        # Translate award levels
        df["awlevel"] = (
            df["awlevel"].astype(int).map(AWARD_LEVEL_CODES).fillna(AwardLevel.unknown.value)
        )

        # Translate CIP codes (and drop generic / total codes)
        totals_mask = (df["cipcode"] == "99.0000") | (df["cipcode"] == "99")
        if verbose:
            logger.info(
                f"Removing {totals_mask.sum():,} rows with CIP code of 99, indicating total"
            )
        df = df[~totals_mask]

        cip2020_df = self.crosswalk.convert_to_cip2020(year, df["cipcode"])
        df = df.assign(cip2020=cip2020_df["cip2020"])

        # Set index columns
        df.set_index(["year"] + INDEX_COLS + ["cip2020"], inplace=True)

        # Translate column CRACE codes
        # If any derived values are in the columns use the interim versions
        if any(col.startswith("dv") for col in df.columns):
            col_map = INTERIM_CRACE_CODES
        elif any(col.startswith("crace") for col in df.columns):
            col_map = PRE_2010_CRACE_CODES
        else:
            col_map = POST_2010_CRACE_CODES

        # Stack race/gender into columns to make tidy
        df = df[list(col_map.keys())]
        df.columns = df.columns.map(col_map)
        df.columns.rename(["race_ethnicity", "gender"], inplace=True)
        series = pd.Series(df.stack([0, 1], future_stack=True))

        # It's a series now so we can rename it
        series.rename("n_awards", inplace=True)

        # Reduce file size by removing rows with zeros
        if remove_zero:
            zero_mask = series == "0"
            if verbose:
                logger.info(f"Removing {zero_mask.sum():,} rows with value of zero")
            series = series[~zero_mask]

        # Return as a dataframe
        return series.to_frame()

    def read_year(
        self, folder: Path, add_ncses: bool = True, add_dhs: bool = True, verbose: bool = True
    ) -> pd.DataFrame:
        try:
            year = int(folder.name)
        except Exception:
            raise Exception(f"Provided path {folder} could not be interpreted as a year.")
        if verbose:
            logger.info(f"Reading completions data for the year {year} in {folder}")
        df = self._read_raw_data_files(folder, verbose=verbose)
        df = self._clean(df, verbose=verbose)
        df = self._translate_transform(df, year=year, verbose=verbose)

        if add_ncses:
            # Classifying the "original" codes works best, a few crosswalked codes are missing
            nc = self.ncses_classifier.classify(df.index.get_level_values("cipcode"))
            df[nc.columns] = nc.values

        if add_dhs:
            # Run DHS classification on old + new CIP codes to backstop against crosswalk issues
            dc = self.dhs_classifier.classify(df.index.get_level_values("cipcode"))
            dc2020 = self.dhs_classifier.classify(df.index.get_level_values("cip2020"))
            dc.dhs_stem = dc.values | dc2020.values
            df[dc.columns] = dc.values

        if verbose:
            logger.info(f"Returning df with {df.shape[0]:,} rows and {df.shape[1]:,} cols")
        return df


def completions(
    survey_dir: Path = pipeline.settings.RAW_DATA_DIR / constants.COMPLETIONS_TABLE,
    output_dir: Path = pipeline.settings.INTERIM_DATA_DIR / constants.COMPLETIONS_TABLE,
    verbose: bool = True,
):
    """Process raw data into interim CSV files with NCSES and DHS STEM classifications"""
    reader = IPEDSCompletionsReader()
    year_dirs = sorted([d for d in survey_dir.iterdir() if d.is_dir()], reverse=True)
    all_unclassified = []
    for year_dir in tqdm(year_dirs):
        df = reader.read_year(year_dir, verbose=verbose)
        csv_dir = output_dir
        csv_dir.mkdir(parents=True, exist_ok=True)
        output_file = csv_dir / (year_dir.name + ".csv.gz")
        df.to_csv(output_file, compression="gzip")
        if verbose:
            logger.info(f"Wrote results for {year_dir.name} to {output_file}")

        # Make note of any CIP codes unclassified by NCSES
        unclassified = df[df.ncses_sci_group == NCSESSciGroup.unknown.value]
        unclassified = unclassified.reset_index()[["cipcode", "cip2020"]].drop_duplicates()
        all_unclassified.append(unclassified)

    if verbose:
        unclassified = pd.concat(all_unclassified)
        if unclassified.empty:
            logger.info("No CIP codes were missing from NCSES classification!")
        else:
            unclassified = unclassified.drop_duplicates()
            logger.info("The following CIP Codes were not classified in NCSES")
            logger.info(unclassified)


if __name__ == "__main__":
    typer.run(completions)
