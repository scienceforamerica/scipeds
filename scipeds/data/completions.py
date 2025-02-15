import warnings
from pathlib import Path
from typing import Optional

import pandas as pd

from scipeds import constants
from scipeds.data.engine import IPEDSQueryEngine
from scipeds.data.enums import FieldTaxonomy, Grouping
from scipeds.data.queries import QueryFilters, TaxonomyRollup
from scipeds.utils import (
    Rate,
    calculate_effect_size,
    calculate_rel_rate,
)


class CompletionsQueryEngine(IPEDSQueryEngine):
    GROUP_QUERY = """
    SELECT DISTINCT {grouping}
        ,COALESCE(
            SUM(CASE WHEN {taxonomy} in (SELECT UNNEST($taxonomy_values)) THEN n_awards ELSE 0 END)
            OVER({subgroup_partition})
            ,0)::INT64 AS rollup_degrees_{label}
        ,COALESCE(
            SUM(CASE WHEN {taxonomy} in (SELECT UNNEST($taxonomy_values)) THEN n_awards ELSE 0 END)
            OVER({total_partition})
            ,0)::INT64 AS rollup_degrees_total
        ,COALESCE(
            SUM(n_awards) 
            OVER({subgroup_partition})
            ,0)::INT64 AS uni_degrees_{label}
        ,COALESCE(
            SUM(n_awards) 
            OVER({total_partition})
            ,0)::INT64 AS uni_degrees_total
    FROM {completions_table}
    WHERE
        year BETWEEN $start_year AND $end_year
        AND awlevel IN (SELECT UNNEST($award_levels))
        AND race_ethnicity IN (SELECT UNNEST($race_ethns))
        AND majornum IN (SELECT UNNEST($majornums))
    ORDER BY {grouping};
    """

    GROUP_FIELDS_QUERY = """
    SELECT DISTINCT {columns}
        ,COALESCE( SUM(n_awards) OVER ({field_group_partition}), 0)::INT64
            AS field_degrees_{label}
        ,COALESCE( SUM(n_awards) OVER ({field_partition}), 0)::INT64 
            AS field_degrees_total
        ,COALESCE( SUM(n_awards) OVER ({group_partition}), 0)::INT64 
            AS uni_degrees_{label}
        ,COALESCE( SUM(n_awards) OVER ({total_partition}), 0)::INT64
            AS uni_degrees_total
    FROM {completions_table}
    WHERE
        year BETWEEN $start_year AND $end_year
        AND awlevel IN (SELECT UNNEST($award_levels))
        AND race_ethnicity IN (SELECT UNNEST($race_ethns))
        AND majornum IN (SELECT UNNEST($majornums))
    ORDER BY {columns};
    """

    UNI_GROUP_QUERY = """
    WITH totals AS (
        SELECT DISTINCT {columns}
            ,COALESCE(
                SUM(n_awards)
                    FILTER(WHERE {taxonomy} in (SELECT UNNEST($taxonomy_values)))
                    OVER({subtotal_partition})
                ,0)::INT64 AS rollup_degrees_{label}
            ,COALESCE( SUM(n_awards) 
                    FILTER(WHERE {taxonomy} in (SELECT UNNEST($taxonomy_values)))
                    OVER({total_partition})
                ,0)::INT64 AS rollup_degrees_total
            ,COALESCE( SUM(n_awards) OVER({subtotal_partition} )
                ,0)::INT64 AS uni_degrees_{label}
            ,COALESCE( SUM(n_awards) OVER({total_partition})
                ,0)::INT64 AS uni_degrees_total
        FROM {completions_table}
        WHERE
            year BETWEEN $start_year AND $end_year
            AND race_ethnicity IN (SELECT UNNEST($race_ethns))
            AND awlevel IN (SELECT UNNEST($award_levels))
            AND majornum IN (SELECT UNNEST($majornums))
    )
    SELECT totals.*, {institutions_table}.*
    FROM totals
    LEFT JOIN {institutions_table}
    USING (unitid)
    ORDER BY totals.{columns};
    """

    UNI_GROUP_FIELDS_QUERY = """
    WITH filtered AS (
        SELECT * FROM {completions_table}
        WHERE 
            year BETWEEN $start_year AND $end_year
            AND race_ethnicity IN (SELECT UNNEST($race_ethns))
            AND awlevel IN (SELECT UNNEST($award_levels))
            AND majornum IN (SELECT UNNEST($majornums))
    ), uni_field_group_totals AS (
        SELECT unitid, {taxonomy}, {columns}
            ,COALESCE(SUM(n_awards), 0)::INT64 AS field_degrees_{label}
        FROM filtered
        {taxonomy_filter}
        GROUP BY unitid, {taxonomy}, {columns}
    ), uni_field_totals AS (
        SELECT unitid, {taxonomy} {year}
            ,COALESCE(SUM(n_awards), 0)::INT64 AS field_degrees_total
        FROM filtered
        {taxonomy_filter}
        GROUP BY unitid, {taxonomy} {year}
    ), uni_group_totals AS (
        SELECT unitid, {columns}
            ,COALESCE(SUM(n_awards), 0)::INT64 AS uni_degrees_{label}
        FROM filtered
        GROUP BY unitid, {columns}
    ), uni_totals AS (
        SELECT unitid {year}
            ,COALESCE(SUM(n_awards), 0)::INT64 AS uni_degrees_total
        FROM filtered
        GROUP BY unitid {year}
    )
    SELECT
        unitid, 
        {taxonomy}, 
        {columns},
        field_degrees_{label}, 
        field_degrees_total,
        uni_degrees_{label},
        uni_degrees_total
    FROM uni_field_group_totals ufgt
    LEFT JOIN uni_field_totals uft 
        USING (unitid, {taxonomy} {year})
    LEFT JOIN uni_group_totals ugt 
        USING (unitid, {columns}) 
    JOIN uni_totals ut 
        USING (unitid {year})
    ORDER BY unitid, {taxonomy}, {columns};
    """

    def __init__(self, db_path: Optional[Path] = constants.SCIPEDS_CACHE_DIR / constants.DB_NAME):
        """A structured way to query the IPEDS completions table

        Args:
            db_path (Optional[Path], optional): Path to pre-processed database file.
                Defaults to constants.CACHE_DIR / constants.DB_NAME.

        Raises:
            FileNotFoundError: Pre-processed database file not found.
        """
        super().__init__(db_path)
        self.groupings = ("race_ethnicity", "gender", "intersectional")

    def _check_rollup_values(self, rollup: TaxonomyRollup):
        """Check whether the rollup values provided exist in the specified taxonomy, and warn
        the user if they do not.

        Args:
            rollup (TaxonomyRollup): Taxonomy rollup
        """
        unique_values = self.get_df_from_query(
            f"SELECT DISTINCT {rollup.taxonomy_name} FROM {constants.COMPLETIONS_TABLE}"
        )[rollup.taxonomy_name]
        missing = [value for value in rollup.taxonomy_values if value not in unique_values.values]
        if (n_missing := len(missing)) > 0:
            warnings.warn(
                f"{n_missing} provided taxonomy values were missing from taxonomy "
                f"{rollup.taxonomy_name}. First {n_missing if n_missing < 3 else 3} "
                f"missing values: {list(missing)[:3]}.\n"
                "The query will still proceed but beware that you might not get "
                "the correct or expected results."
            )

    def rollup_by_grouping(
        self,
        grouping: str,
        rollup: TaxonomyRollup,
        query_filters: QueryFilters,
        by_year: bool = False,
        rel_rate: bool = False,
    ) -> pd.DataFrame:
        """Aggregate completions (subject to filters) for fields within the given roll-up,
        aggregating by selected grouping and subject to the applied filters

        Args:
            grouping (str): Variable to group by (either "gender" or "race_ethnicity")
            rollup (TaxonomyRollup): Fields in taxonomy to include in aggregation
            query_filters (QueryFilters): Filters to apply prior to aggregation
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. If true,
                also adds associated variables. Default: False

        Returns:
            pd.DataFrame: Completions within fields in the roll-up,
                aggregated by chosen grouping and subject to filters
        """
        # Add year to the group by if we want a timeseries
        col_select = ["year"] if by_year else []

        # Change the column labels for easier parsing based on the grouping
        if grouping == "race_ethnicity" or grouping == "gender":
            col_select = [grouping] + col_select
            label = f"within_{grouping}"
        elif grouping == "intersectional":
            col_select = ["race_ethnicity", "gender"] + col_select
            label = "intersectional"
        else:
            raise ValueError(
                f"Provided grouping {grouping} not allowed; allowed values are {self.groupings}"
            )

        # Warn the user if rollup values are missing from the specified taxonomy
        self._check_rollup_values(rollup)

        # Format and execute the query
        grouping_columns = ",".join(col_select)
        subgroup_partition = f"PARTITION BY {grouping_columns}"
        total_partition = "PARTITION BY year" if by_year else ""
        query = self.GROUP_QUERY.format(
            completions_table=constants.COMPLETIONS_TABLE,
            grouping=grouping_columns,
            subgroup_partition=subgroup_partition,
            total_partition=total_partition,
            taxonomy=rollup.taxonomy_name,
            label=label,
        )
        query_params = query_filters.model_dump()
        query_params.update(rollup.model_dump(include={"taxonomy_values"}))
        df = self.get_df_from_query(query, query_params=query_params)

        # Calculate relative rate
        if rel_rate:
            stem_pct = Rate("field_pct", f"rollup_degrees_{label}", "rollup_degrees_total")
            uni_pct = Rate("uni_pct", f"uni_degrees_{label}", "uni_degrees_total")
            df = calculate_rel_rate(df, stem_pct, uni_pct)

        return df.set_index(col_select)

    def field_totals_by_grouping(
        self,
        grouping: str,
        taxonomy: FieldTaxonomy,
        query_filters: QueryFilters,
        by_year: bool = False,
        rel_rate: bool = False,
    ) -> pd.DataFrame:
        """Compute aggregate counts for all fields in a given taxonomy

        Args:
            grouping (str): Either "race_ethnicity", "gender", or "intersectional
            taxonomy (FieldTaxonomy): Taxonomy to aggregate over
            query_filters (QueryFilters): Pre-aggregation filters to apply to raw data
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. Default: False

        Returns:
            pd.DataFrame: Relative rates by grouping for each field in taxonomy
        """
        # Group by year if specified
        year_col = ["year"] if by_year else []

        # Format column names nicely
        if grouping == "race_ethnicity" or grouping == "gender":
            label = f"within_{grouping}"
            grouping_cols = [grouping]
        elif grouping == "intersectional":
            label = "intersectional"
            grouping_cols = ["race_ethnicity", "gender"]
        else:
            raise ValueError(
                f"Provided grouping {grouping} not allowed; allowed values are {self.groupings}"
            )

        # Populate and execute the query
        base_columns = [taxonomy] + year_col
        group_columns = grouping_cols + year_col
        all_columns = [taxonomy] + grouping_cols + year_col
        field_group_partition = "PARTITION BY " + ", ".join(all_columns)
        field_partition = "PARTITION BY " + ", ".join(base_columns)
        group_partition = "PARTITION BY " + ", ".join(group_columns)
        total_partition = "PARTITION BY year" if by_year else ""

        query = self.GROUP_FIELDS_QUERY.format(
            completions_table=constants.COMPLETIONS_TABLE,
            columns=", ".join(all_columns),
            field_group_partition=field_group_partition,
            field_partition=field_partition,
            group_partition=group_partition,
            total_partition=total_partition,
            label=label,
        )
        query_params = query_filters.model_dump()
        df = self.get_df_from_query(query, query_params=query_params)

        if rel_rate:
            # Calculate relative rate
            field_pct = Rate("field_pct", f"field_degrees_{label}", "field_degrees_total")
            uni_pct = Rate("uni_pct", f"uni_degrees_{label}", "uni_degrees_total")
            df = calculate_rel_rate(df, field_pct, uni_pct)

        return df.set_index(all_columns)

    def uni_rollup_by_grouping(
        self,
        grouping: str,
        rollup: TaxonomyRollup,
        query_filters: QueryFilters,
        by_year: bool = False,
        rel_rate: bool = False,
        effect_size: bool = False,
    ) -> pd.DataFrame:
        """Get intersectional degree counts and rates within intersectional subgroups"

        Args:
            grouping (str): Either "race_ethnicity" or "gender"
            rollup (TaxonomyRollup): Taxonomy to aggregate over
            query_filters (QueryFilters): Pre-aggregation filters
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. If true,
                also adds associated variables. Default: False
            effect_size (bool): Whether to compute effect size. Default: False

        Returns:
            pd.DataFrame: Completions in fields contained within roll-up, aggregated by
                university UNITID and chosen grouping, subject to filters
        """
        if grouping == "race_ethnicity" or grouping == "gender":
            label = f"within_{grouping}"
            grouping_cols = [grouping]
        elif grouping == "intersectional":
            label = "intersectional"
            grouping_cols = ["race_ethnicity", "gender"]
        else:
            raise ValueError(
                f"Provided grouping {grouping} not allowed; allowed values are {self.groupings}"
            )

        # Warn the user if rollup values are missing from the specified taxonomy
        self._check_rollup_values(rollup)

        # Format and execute query
        year_col = ["year"] if by_year else []
        base_columns = ["unitid"] + year_col
        all_columns = ["unitid"] + grouping_cols + year_col

        subtotal_partition = "PARTITION BY " + ", ".join(all_columns)
        total_partition = "PARTITION BY " + ", ".join(base_columns)
        query = self.UNI_GROUP_QUERY.format(
            completions_table=constants.COMPLETIONS_TABLE,
            institutions_table=constants.INSTITUTIONS_TABLE,
            taxonomy=rollup.taxonomy_name,
            columns=", ".join(all_columns),
            label=label,
            subtotal_partition=subtotal_partition,
            total_partition=total_partition,
        )
        query_params = query_filters.model_dump()
        query_params.update(rollup.model_dump(include={"taxonomy_values"}))
        df = self.get_df_from_query(query, query_params=query_params)

        stem_pct = Rate("stem_pct", f"rollup_degrees_{label}", "rollup_degrees_total")
        uni_pct = Rate("uni_pct", f"uni_degrees_{label}", "uni_degrees_total")

        if rel_rate or effect_size:
            # Calculate z-score for rollup field pct relative to baseline uni pct
            df = calculate_rel_rate(df, stem_pct, uni_pct)

        if effect_size:
            df = calculate_effect_size(df, stem_pct, uni_pct, group_cols=all_columns[1:])

        return df.set_index(all_columns)

    def uni_field_totals_by_grouping(
        self,
        grouping: Grouping,
        taxonomy: FieldTaxonomy,
        query_filters: QueryFilters,
        taxonomy_values: list[str] | None = None,
        by_year: bool = False,
        rel_rate: bool = False,
        effect_size: bool = False,
        show_query: bool = False,
    ) -> pd.DataFrame:
        """Aggregate completions (subject to filters) for all fields
        within a given taxonomy at each university

        Args:
            grouping (str): Either "race_ethnicity", "gender", or "intersectional
            taxonomy (FieldTaxonomy): Taxonomy to aggregate over
            query_filters (QueryFilters): Pre-aggregation filters to apply to raw data
            taxonomy_values (list[str]): Optional list of field values to filter on. Default: None
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. If true,
                also adds associated variables. Default: False
            effect_size (bool): Whether to compute effect size. Default: False

        Returns:
            pd.DataFrame: Completions in each field in the taxonomy, aggregated by
                university UNITID and chosen grouping, subject to filters
        """
        label = grouping.label_suffix
        columns = grouping.grouping_columns
        if by_year:
            columns.append("year")
        taxonomy_filter = (
            f"WHERE {taxonomy} IN (SELECT UNNEST($taxonomy_values))" if taxonomy_values else ""
        )

        query = self.UNI_GROUP_FIELDS_QUERY.format(
            completions_table=constants.COMPLETIONS_TABLE,
            institutions_table=constants.INSTITUTIONS_TABLE,
            taxonomy=taxonomy,
            taxonomy_filter=taxonomy_filter,
            columns=", ".join(columns),
            label=label,
            year=", year" if by_year else "",
        )
        query_params = query_filters.model_dump()
        if taxonomy_values:
            query_params["taxonomy_values"] = taxonomy_values

        df = self.get_df_from_query(query, query_params=query_params, show_query=show_query)

        field_pct = Rate("field_pct", f"field_degrees_{label}", "field_degrees_total")
        uni_pct = Rate("uni_pct", f"uni_degrees_{label}", "uni_degrees_total")

        if rel_rate or effect_size:
            df = calculate_rel_rate(df, field_pct, uni_pct)

        index_cols = ["unitid", taxonomy, *columns]
        if effect_size:
            df = calculate_effect_size(df, field_pct, uni_pct, group_cols=index_cols[1:])

        return df.set_index(index_cols)
