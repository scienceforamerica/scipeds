import warnings

import pandas as pd

from scipeds.constants import COMPLETIONS_TABLE, INSTITUTIONS_TABLE
from scipeds.data.engine import IPEDSQueryEngine
from scipeds.data.enums import FieldTaxonomy, Grouping
from scipeds.data.queries import QueryFilters, TaxonomyRollup
from scipeds.utils import (
    Rate,
    calculate_effect_size,
    calculate_rel_rate,
)


class CompletionsQueryEngine(IPEDSQueryEngine):
    GROUP_FIELDS_QUERY = """
WITH filtered AS (
    SELECT * FROM {completions_table}
    WHERE 
        year BETWEEN $start_year AND $end_year
        AND race_ethnicity IN (SELECT UNNEST($race_ethns))
        AND awlevel IN (SELECT UNNEST($award_levels))
        AND majornum IN (SELECT UNNEST($majornums))
        {unitid_filter}
), 
field_totals AS (
    {field_total_select}
), 
group_totals AS (
    {group_total_select}
), 
valid_combinations AS (
    {valid_combo_select}
),
field_group_totals AS (
    {field_group_total_select}
), 
totals AS (
    {total_select}
)
SELECT
    {institution_name}
    {field_group_cols},
    COALESCE({field_group_degrees}, 0)::INT64 as {field_group_degrees},
    {field_total_degrees},
    {group_total_degrees},
    {total_degrees},
FROM valid_combinations
{joins}
ORDER BY {field_group_cols};"""

    def _check_rollup_values(self, rollup: TaxonomyRollup):
        """Check whether the rollup values provided exist in the specified taxonomy, and warn
        the user if they do not.

        Args:
            rollup (TaxonomyRollup): Taxonomy rollup
        """
        unique_values = self.get_df_from_query(
            f"SELECT DISTINCT {rollup.taxonomy_name} FROM {COMPLETIONS_TABLE}"
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

    def _format_query(
        self,
        grouping: Grouping,
        agg_type: str,
        field_group_cols: list[str],
        field_total_cols: list[str],
        group_total_cols: list[str],
        total_cols: list[str],
        taxonomy: str | None = None,
        unitids: list[int] | None = None,
    ) -> str:
        """Format the query according to the provided arguments

        Args:
            grouping (Grouping): How to group the data
            agg_type (str): Type of aggregation ("rollup" or "field")
            field_group_cols (list[str]): Lowest level of aggregation
            field_total_cols (list[str]): Field-only aggregation
            group_total_cols (list[str]): Grouping-only aggregation
            total_cols (list[str]): Highest level of aggregation
            taxonomy (str, Optional): Taxonomy to filter on. Default: None
            unitids (list[int], Optional): List of unitids to filter on. Default: None

        Returns:
            str: Formatted query
        """
        taxonomy_filter = (
            f"WHERE {taxonomy} IN (SELECT UNNEST($taxonomy_values))" if taxonomy else ""
        )
        unitid_filter = (
            f"AND unitid IN ({', '.join([str(unitid) for unitid in unitids])})" if unitids else ""
        )

        def format_subquery(cols: list[str], name: str, filter: str = "") -> str:
            selection = ", ".join(cols) + "," if cols else ""
            groupby = f"GROUP BY {', '.join(cols)}" if cols else ""
            return (
                f"SELECT {selection} "
                f"  COALESCE(SUM(n_awards), 0)::INT64 AS {name} "
                f"FROM filtered {filter} "
                f"{groupby}"
            )

        field_group_total_select = format_subquery(
            field_group_cols, f"{agg_type}_degrees_{grouping.label_suffix}", taxonomy_filter
        )
        field_total_select = format_subquery(
            field_total_cols, f"{agg_type}_degrees_total", taxonomy_filter
        )
        field_total_cols = field_total_cols + [f"{agg_type}_degrees_total"]
        group_total_select = format_subquery(
            group_total_cols, f"uni_degrees_{grouping.label_suffix}"
        )
        group_total_cols = group_total_cols + [f"uni_degrees_{grouping.label_suffix}"]
        total_select = format_subquery(total_cols, "uni_degrees_total")

        combo_using = f"USING ({','.join(total_cols)})" if total_cols else ""
        combo_cols = set(group_total_cols) - set(field_total_cols)
        field_cols = ["field_totals." + col for col in field_total_cols]
        group_cols = ["group_totals." + col for col in combo_cols]
        valid_combo_select = f"""
        SELECT {", ".join([*field_cols]) if field_cols else "field_totals.*"}
        ,{", ".join([*group_cols])}
        FROM field_totals
        {"CROSS" if not combo_using else ""} JOIN group_totals {combo_using}
        """

        def format_join(cols: list[str], var_name: str, table_name: str) -> tuple[str, str]:
            if cols:
                join_str = f"LEFT JOIN {table_name} USING ({', '.join(cols)})"
            else:
                var_name = f"{table_name}.{var_name}"
                join_str = f",{table_name}"
            return var_name, join_str

        fgt_var, fgt_join = format_join(
            field_group_cols, f"{agg_type}_degrees_{grouping.label_suffix}", "field_group_totals"
        )
        ft_var, _ = format_join(
            field_total_cols, f"{agg_type}_degrees_total", "valid_combinations"
        )
        gt_var, _ = format_join(
            group_total_cols, f"uni_degrees_{grouping.label_suffix}", "valid_combinations"
        )
        t_var, t_join = format_join(total_cols, "uni_degrees_total", "totals")

        all_joins = [fgt_join, t_join]

        if "unitid" in total_cols:
            all_joins.append(f"LEFT JOIN {INSTITUTIONS_TABLE} USING (unitid)")

        joins = "\n".join(all_joins)

        institution_name = (
            f"{INSTITUTIONS_TABLE}.institution_name," if "unitid" in total_cols else ""
        )

        query = self.GROUP_FIELDS_QUERY.format(
            completions_table=COMPLETIONS_TABLE,
            institution_name=institution_name,
            unitid_filter=unitid_filter,
            field_group_cols=", ".join(field_group_cols),
            field_group_total_select=field_group_total_select,
            field_total_select=field_total_select,
            group_total_select=group_total_select,
            valid_combo_select=valid_combo_select,
            total_select=total_select,
            field_group_degrees=fgt_var,
            field_total_degrees=ft_var,
            group_total_degrees=gt_var,
            total_degrees=t_var,
            joins=joins,
        )

        return query

    def rollup_by_grouping(
        self,
        grouping: Grouping,
        rollup: TaxonomyRollup,
        query_filters: QueryFilters,
        by_year: bool = False,
        rel_rate: bool = False,
        show_query: bool = False,
        filter_unitids: list[int] | None = None,
    ) -> pd.DataFrame:
        """Aggregate completions (subject to filters) for fields within the given roll-up,
        aggregating by selected grouping and subject to the applied filters

        Args:
            grouping (Grouping): How to group the data
            rollup (TaxonomyRollup): Fields in taxonomy to include in aggregation
            query_filters (QueryFilters): Filters to apply prior to aggregation
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. If true,
                also adds associated variables. Default: False
            show_query (bool): Whether to print the query and parameters before executing.
                Default: False
            filter_unitids (list[int], Optional): List of unitids to filter on. Default: None

        Returns:
            pd.DataFrame: Completions within fields in the roll-up,
                aggregated by chosen grouping and subject to filters
        """
        # Validate inputs
        grouping = Grouping(grouping)

        # Warn the user if rollup values are missing from the specified taxonomy
        self._check_rollup_values(rollup)

        year = ["year"] if by_year else []
        field_group_cols = [*grouping.grouping_columns, *year]
        field_total_cols = [*year]
        group_total_cols = [*grouping.grouping_columns, *year]
        total_cols = [*year]

        # Format and execute the query
        query = self._format_query(
            grouping=grouping,
            agg_type="rollup",
            field_group_cols=field_group_cols,
            field_total_cols=field_total_cols,
            group_total_cols=group_total_cols,
            total_cols=total_cols,
            taxonomy=rollup.taxonomy_name,
            unitids=filter_unitids,
        )
        query_params = query_filters.model_dump()
        query_params.update(rollup.model_dump(include={"taxonomy_values"}))
        df = self.get_df_from_query(query, query_params=query_params, show_query=show_query)

        # Calculate relative rate
        if rel_rate:
            rollup_pct = Rate(
                "rollup_pct", f"rollup_degrees_{grouping.label_suffix}", "rollup_degrees_total"
            )
            uni_pct = Rate("uni_pct", f"uni_degrees_{grouping.label_suffix}", "uni_degrees_total")
            df = calculate_rel_rate(df, rollup_pct, uni_pct)

        return df.set_index(field_group_cols)

    def field_totals_by_grouping(
        self,
        grouping: Grouping,
        taxonomy: FieldTaxonomy,
        query_filters: QueryFilters,
        taxonomy_values: list[str] | None = None,
        by_year: bool = False,
        rel_rate: bool = False,
        show_query: bool = False,
        filter_unitids: list[int] | None = None,
    ) -> pd.DataFrame:
        """Compute aggregate counts for all fields in a given taxonomy

        Args:
            grouping (Grouping): How to group the data
            taxonomy (FieldTaxonomy): Taxonomy to aggregate over
            query_filters (QueryFilters): Pre-aggregation filters to apply to raw data
            taxonomy_values (list[str]): Optional list of field values to filter on. Default: None
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. Default: False
            show_query (bool): Whether to print the query and parameters before executing.
                Default: False
            filter_unitids (list[int], Optional): List of unitids to filter on. Default: None

        Returns:
            pd.DataFrame: Relative rates by grouping for each field in taxonomy
        """
        # Validate inputs
        grouping = Grouping(grouping)
        taxonomy = FieldTaxonomy(taxonomy)

        year = ["year"] if by_year else []
        field_group_cols = [taxonomy.value, *grouping.grouping_columns, *year]
        field_total_cols = [taxonomy.value, *year]
        group_total_cols = [*grouping.grouping_columns, *year]
        total_cols = [*year]

        query = self._format_query(
            grouping=grouping,
            agg_type="field",
            field_group_cols=field_group_cols,
            field_total_cols=field_total_cols,
            group_total_cols=group_total_cols,
            total_cols=total_cols,
            taxonomy=taxonomy if taxonomy_values else None,
            unitids=filter_unitids,
        )
        query_params = query_filters.model_dump()
        if taxonomy_values:
            query_params["taxonomy_values"] = taxonomy_values
        df = self.get_df_from_query(query, query_params=query_params, show_query=show_query)

        if rel_rate:
            # Calculate relative rate
            field_pct = Rate(
                "field_pct", f"field_degrees_{grouping.label_suffix}", "field_degrees_total"
            )
            uni_pct = Rate("uni_pct", f"uni_degrees_{grouping.label_suffix}", "uni_degrees_total")
            df = calculate_rel_rate(df, field_pct, uni_pct)

        return df.set_index(field_group_cols)

    def uni_rollup_by_grouping(
        self,
        grouping: Grouping,
        rollup: TaxonomyRollup,
        query_filters: QueryFilters,
        by_year: bool = False,
        rel_rate: bool = False,
        effect_size: bool = False,
        show_query: bool = False,
        filter_unitids: list[int] | None = None,
    ) -> pd.DataFrame:
        """Get intersectional degree counts and rates within intersectional subgroups"

        Args:
            grouping (Grouping): How to group the data
            rollup (TaxonomyRollup): Taxonomy to aggregate over
            query_filters (QueryFilters): Pre-aggregation filters
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. If true,
                also adds associated variables. Default: False
            effect_size (bool): Whether to compute effect size. Default: False
            show_query (bool): Whether to print the query and parameters before executing.
                Default: False
            filter_unitids (list[int], Optional): List of unitids to filter on. Default: None

        Returns:
            pd.DataFrame: Completions in fields contained within roll-up, aggregated by
                university UNITID and chosen grouping, subject to filters
        """
        # Validate inputs
        grouping = Grouping(grouping)

        # Warn the user if rollup values are missing from the specified taxonomy
        self._check_rollup_values(rollup)

        year = ["year"] if by_year else []
        field_group_cols = ["unitid", *grouping.grouping_columns, *year]
        field_total_cols = ["unitid", *year]
        group_total_cols = ["unitid", *grouping.grouping_columns, *year]
        total_cols = ["unitid", *year]

        query = self._format_query(
            grouping=grouping,
            agg_type="rollup",
            field_group_cols=field_group_cols,
            field_total_cols=field_total_cols,
            group_total_cols=group_total_cols,
            total_cols=total_cols,
            taxonomy=rollup.taxonomy_name,
            unitids=filter_unitids,
        )
        query_params = query_filters.model_dump()
        query_params.update(rollup.model_dump(include={"taxonomy_values"}))
        df = self.get_df_from_query(query, query_params=query_params, show_query=show_query)

        rollup_pct = Rate(
            "rollup_pct", f"rollup_degrees_{grouping.label_suffix}", "rollup_degrees_total"
        )
        uni_pct = Rate("uni_pct", f"uni_degrees_{grouping.label_suffix}", "uni_degrees_total")

        if rel_rate or effect_size:
            # Calculate z-score for rollup field pct relative to baseline uni pct
            df = calculate_rel_rate(df, rollup_pct, uni_pct)

        if effect_size:
            df = calculate_effect_size(df, rollup_pct, uni_pct, group_cols=field_group_cols[1:])

        return df.set_index(["institution_name"] + field_group_cols)

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
        filter_unitids: list[int] | None = None,
    ) -> pd.DataFrame:
        """Aggregate completions (subject to filters) for all fields
        within a given taxonomy at each university

        Args:
            grouping (Grouping): How to group the data
            taxonomy (FieldTaxonomy): Taxonomy to aggregate over
            query_filters (QueryFilters): Pre-aggregation filters to apply to raw data
            taxonomy_values (list[str]): Optional list of field values to filter on. Default: None
            by_year (bool): Whether to group by year (True) or aggregate over all years (False).
                Default: False
            rel_rate (bool): Whether to calculate relative representation. If true,
                also adds associated variables. Default: False
            effect_size (bool): Whether to compute effect size. Default: False
            show_query (bool): Whether to print the query and parameters before executing.
                Default: False
            filter_unitids (list[int], Optional): List of unitids to filter on. Default: None

        Returns:
            pd.DataFrame: Completions in each field in the taxonomy, aggregated by
                university UNITID and chosen grouping, subject to filters
        """
        # Validate inputs
        grouping = Grouping(grouping)
        taxonomy = FieldTaxonomy(taxonomy)

        year = ["year"] if by_year else []
        field_group_cols = ["unitid", taxonomy.value, *grouping.grouping_columns, *year]
        field_total_cols = ["unitid", taxonomy.value, *year]
        group_total_cols = ["unitid", *grouping.grouping_columns, *year]
        total_cols = ["unitid", *year]

        query = self._format_query(
            grouping=grouping,
            agg_type="field",
            field_group_cols=field_group_cols,
            field_total_cols=field_total_cols,
            group_total_cols=group_total_cols,
            total_cols=total_cols,
            taxonomy=taxonomy if taxonomy_values else None,
            unitids=filter_unitids,
        )

        query_params = query_filters.model_dump()
        if taxonomy_values:
            query_params["taxonomy_values"] = taxonomy_values

        df = self.get_df_from_query(query, query_params=query_params, show_query=show_query)

        field_pct = Rate(
            "field_pct", f"field_degrees_{grouping.label_suffix}", "field_degrees_total"
        )
        uni_pct = Rate("uni_pct", f"uni_degrees_{grouping.label_suffix}", "uni_degrees_total")

        if rel_rate or effect_size:
            df = calculate_rel_rate(df, field_pct, uni_pct)

        if effect_size:
            df = calculate_effect_size(df, field_pct, uni_pct, group_cols=field_group_cols[1:])

        return df.set_index(["institution_name"] + field_group_cols)
