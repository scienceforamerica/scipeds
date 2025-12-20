import pandas as pd

from scipeds.constants import ENROLLMENT_RESIDENCE_TABLE, INSTITUTIONS_TABLE
from scipeds.data.engine import IPEDSQueryEngine
from scipeds.data.queries import FallEnrollmentQueryFilters


class FallEnrollmentQueryEngine(IPEDSQueryEngine):
    RESIDENCE_QUERY = """
WITH filtered AS (
    SELECT * FROM {enrollment_residence_table}
    WHERE 
        year BETWEEN $start_year AND $end_year
        AND state_of_residence IN (SELECT UNNEST($geos))
        {unitid_filter}
) 
SELECT
    {institution_name},
    filtered.*
FROM filtered
{joins};
"""

    def residence_by_uni(
        self,
        query_filters: FallEnrollmentQueryFilters,
        show_query: bool = False,
        filter_unitids: list[int] | None = None,
    ) -> pd.DataFrame:
        unitid_filter = (
            f"AND unitid IN ({', '.join([str(unitid) for unitid in filter_unitids])})"
            if filter_unitids
            else ""
        )

        joins = f"LEFT JOIN {INSTITUTIONS_TABLE} USING (unitid)"

        institution_name = f"{INSTITUTIONS_TABLE}.institution_name"

        query = self.RESIDENCE_QUERY.format(
            enrollment_residence_table=ENROLLMENT_RESIDENCE_TABLE,
            institution_name=institution_name,
            unitid_filter=unitid_filter,
            joins=joins,
        )

        query_params = query_filters.model_dump()
        df = self.get_df_from_query(query, query_params=query_params, show_query=show_query)

        col_order = ["year"] + [c for c in df.columns if c != "year"]

        return df[col_order]

    # TODO: make a query that calculates in state vs out of state by uni
