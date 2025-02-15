from pathlib import Path
from typing import Collection, Dict

import pandas as pd
import typer
from tqdm import tqdm

import pipeline.settings
from pipeline.settings import logger
from scipeds import constants
from scipeds.utils import clean_name


class IPEDSInstitutionCharacteristicsReader:
    """Class for reading IPEDS institution characteristics

    There are two translations we need to do:
      1. Translate the shortened/abbreviated variable names (var_name) to their corresponding
         longer description (var_label)
      2. Translate the enumerated values of codes (code_val) to their corresponding string values
         (code_label)"""

    # Details for variables (the Excel sheet where definitions live
    # and the columns for constructing key-value pairs)
    VAR_SHEET = "varlist"
    VAR_NAME_COL = "varname"
    VAR_LABEL_COL = "varTitle"

    # The name of the Excel sheet where the code name -> label dictionary
    # lives for each variable type
    CODE_SHEET = "Frequencies"
    CODE_VAL_COL = "codevalue"
    CODE_LABEL_COL = "valuelabel"

    def __init__(self, keep_raw_vars: Collection = ("STABBR",)):
        """Constructor

        Args:
            keep_raw_vars (Collection, optional): Name of variables to keep
                un-translated as their raw values. Defaults to None.
        """
        self.keep_raw_vars = keep_raw_vars

    def _read_raw_datafile(self, folder: Path, verbose: bool = True) -> pd.DataFrame:
        """Find and read the raw metadata data file from folder"""
        csv_files = list(folder.glob("*.csv"))
        if (n_csvs := len(csv_files)) != 1:
            raise FileNotFoundError(f"Found {n_csvs} csvs in {folder}, expected 1.")
        raw_file = csv_files[0]
        raw_df = pd.read_csv(raw_file, encoding="ISO-8859-1", dtype=str)
        if verbose:
            logger.info(
                f"Read raw data with {raw_df.shape[0]:,} rows and {raw_df.shape[1]:,} cols"
            )
        # strip all whitespace
        for col in raw_df.columns:
            raw_df[col] = raw_df[col].str.strip()
        return raw_df

    def _find_datadict_file(self, folder: Path) -> Path:
        """Find and read the metadata data dictionary from folder"""
        xls_files = list(folder.glob("*.xls"))
        xlsx_files = list(folder.glob("*.xlsx"))
        excel_files = xls_files + xlsx_files
        if (n_files := len(excel_files)) != 1:
            raise FileNotFoundError(f"Found {n_files} Excel files in {folder}, expected 1.")
        return excel_files[0]

    def _get_code_dict(self, filepath: Path, verbose: bool = True) -> dict:
        """Get the dictionary mapping of code values to code labels for each variable"""
        raw_dd = pd.read_excel(filepath, sheet_name=self.CODE_SHEET)
        if verbose:
            logger.info(
                f"Read data dictionary with {raw_dd.shape[0]:,} rows and {raw_dd.shape[1]:,} cols"
            )
        if self.VAR_NAME_COL not in raw_dd.columns:
            raise KeyError("Data dictionary did not contain a column for varname.")

        # Create mappings
        data_dict = {}
        for var_name, var_values in raw_dd.groupby(self.VAR_NAME_COL):
            if var_name not in self.keep_raw_vars:
                code_map = dict(
                    zip(var_values[self.CODE_VAL_COL], var_values[self.CODE_LABEL_COL]),
                )
                data_dict[var_name] = code_map
        return data_dict

    def _get_varname_dict(self, filepath: Path, verbose: bool = True) -> dict:
        """Get the dictionary of variable names -> variable labels

        Also clean the names so that in the database we have readable
        and nicely formatted column names,
        e.g. 'ZIP' -> 'ZIP code' -> 'zip_code'
        """
        raw_varnames = pd.read_excel(filepath, sheet_name=self.VAR_SHEET)
        raw_varnames = raw_varnames[raw_varnames[self.VAR_NAME_COL] != "UNITID"]
        varname_dict = dict(
            zip(
                raw_varnames[self.VAR_NAME_COL], raw_varnames[self.VAR_LABEL_COL].apply(clean_name)
            )
        )
        # Manually override the label for institution name which is 'Institution (entity) name'
        varname_dict["INSTNM"] = "institution_name"
        # Manually override the label for UNITID which is
        # 'Unique identification number of the institution'
        # This keeps the name consistent with the completions table
        varname_dict["UNITID"] = "unitid"
        return varname_dict

    def _translate(
        self, raw_df: pd.DataFrame, code_dict: Dict[str, dict], varname_dict: Dict[str, str]
    ) -> pd.DataFrame:
        """Translate a dataframe using a data dict"""
        # Convert codes
        for col, mapping in code_dict.items():
            raw_df[col] = raw_df[col].map(mapping)
            if raw_df[col].isnull().all():
                logger.warning(f"Column {col} was all null after mapping.")

        # Convert varnames
        raw_df.rename(columns=varname_dict, inplace=True)

        return raw_df

    def _add_custom_vars(self, df: pd.DataFrame) -> pd.DataFrame:
        # Add "Tech School" classification
        tech_strings = ["Polytech", "techn", "school of mines", "Technology", "Engineering"]
        df["tech_school"] = df["institution_name"].str.contains(
            "|".join(tech_strings), case=False, regex=True
        )

        # Construct a column that looks for health science shools
        med_strings = ["Health Science", "Medical Science"]
        df["health_school"] = df["institution_name"].str.contains(
            "|".join(med_strings), case=False, regex=True
        )

        return df

    def read_institution_characteristics(self, folder: Path, verbose: bool = True) -> pd.DataFrame:
        """Read, process, and write interim CSV for IPEDS institution characteristics"""
        df = self._read_raw_datafile(folder, verbose=verbose)
        df["metadata_vintage"] = int(folder.name)
        dd_file = self._find_datadict_file(folder)
        code_dict = self._get_code_dict(dd_file, verbose=verbose)
        varname_dict = self._get_varname_dict(dd_file, verbose=verbose)

        df = self._translate(df, code_dict, varname_dict)
        df = self._add_custom_vars(df)
        return df


def institution_characteristics(
    metadata_dir: Path = pipeline.settings.RAW_DATA_DIR / constants.INSTITUTIONS_TABLE,
    output_dir: Path = pipeline.settings.INTERIM_DATA_DIR / constants.INSTITUTIONS_TABLE,
    verbose: bool = True,
):
    """CLI Entrypoint for processing IPEDS institution characteristics"""
    reader = IPEDSInstitutionCharacteristicsReader()
    year_dirs = sorted([d for d in metadata_dir.iterdir() if d.is_dir()])
    dfs = []

    for year_dir in tqdm(year_dirs):
        if verbose:
            logger.info(f"Reading institutions characteristics data from {year_dir}")
        df = reader.read_institution_characteristics(year_dir, verbose=verbose)
        dfs.append(df)

    if verbose:
        logger.info(
            "Combining institutions characteristics directory info across years, "
            "using most recently available data."
        )
    # Combine and fill null data with most recently available data
    combined = pd.concat(dfs).sort_values("metadata_vintage")
    combined.update(combined.groupby("unitid").ffill())
    most_recent = combined.groupby("unitid").tail(1).sort_values("unitid")

    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "institution_characteristics.csv.gz"
    most_recent.to_csv(output_file, index=False, compression="gzip")
    if verbose:
        logger.info(f"Wrote combined institutions characteristics data to {output_file}")


if __name__ == "__main__":
    typer.run(institution_characteristics)
