from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd

from scipeds.constants import CIP_TABLE, DB_NAME, INSTITUTIONS_TABLE, SCIPEDS_CACHE_DIR


class IPEDSQueryEngine:
    def __init__(self, db_path: Optional[Path] = SCIPEDS_CACHE_DIR / DB_NAME):
        """A structured way to query the IPEDS table to format data for visualization

        Args:
            db_path (Optional[Path], optional): Path to pre-processed database file.
                Defaults to CACHE_DIR / DB_NAME.

        Raises:
            FileNotFoundError: Pre-processed database file not found.
        """
        if db_path and not db_path.exists():
            raise FileNotFoundError(
                f"No db file found at {db_path}! "
                "Specify an existing pre-processed db file "
                "or run scipeds.download_db() to download."
            )

        self.db_path = str(db_path)

    def get_df_from_query(
        self, query: str, query_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Return the dataframe result of the provided SQL query on the pre-processed duckdb

        Args:
            query (str): SQL query (using duckdb syntax)
            query_params (Dict[str, Any], optional): Prepared statement variables for query.
                Defaults to None.

        Returns:
            pd.DataFrame: Data returned by query
        """
        with duckdb.connect(self.db_path, read_only=True) as con:
            if query_params is not None:
                df = con.execute(query, query_params).df()
            else:
                df = con.sql(query).df()
        return df

    def list_tables(self) -> List[str]:
        """List all tables in the duckdb

        Returns:
            List[str]: A list of all available tables
        """
        tables = self.get_df_from_query("SHOW TABLES").iloc[:, 0].values.tolist()
        # Temporary numpy typing fix - see https://github.com/numpy/numpy/issues/27944
        return tables  # type: ignore[return-value]

    def get_cip_table(self) -> pd.DataFrame:
        """Get a table of every unique 2020 CIP Code

        Returns:
            pd.DataFrame: Data frame of CIP codes and corresponding taxonomy titles
        """
        cip_codes = self.get_df_from_query(f"SELECT * FROM {CIP_TABLE}").set_index("cip2020")
        return cip_codes

    def get_institutions_table(self, cols: str | list[str] | None = None) -> pd.DataFrame:
        """Get institution characteristics table, optionally with specified columns

        Returns:
            pd.DataFrame: Data frame of institution characteristics
        """
        inst_df = self.get_df_from_query(f"SELECT * FROM {INSTITUTIONS_TABLE}").set_index("unitid")
        if isinstance(cols, str):
            cols = [cols]
        if cols is None:
            return inst_df
        for col in inst_df.columns:
            assert col in inst_df.columns, f"Invalid column name(s) provided: {cols}"
        return inst_df[cols]
