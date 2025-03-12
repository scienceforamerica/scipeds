# Frequently Asked Questions

## General

### What is `scipeds`?

`scipeds` is a Python library for working with IPEDS data.

### Who made `scipeds` and why did they make it?

`scipeds` was created by Science for America. As part of Science for America's work on STEM equity, we started working with IPEDS data and found ourselves writing code to make our work easier. We realized that most of the code we'd written wasn't specific to our own research questions, and decided to create `scipeds` to make it easier for ourselves and others to work with IPEDS data. 

## Data

### How does `scipeds` query the data?

`scipeds` pre-processes raw data from IPEDS into a duckdb database file, which users download the first time they get set up with `scipeds`. Functions are then implemented in various `QueryEngine`s to make it easy to aggregate data in common ways without having to write a ton of SQL queries.

### Can I re-produce the pre-processed database for myself?

Yes! By cloning the GitHub repository and running the data pipeline.

### What data is currently included in `scipeds`?

`scipeds` currently incorporates the following IPEDS survey components: 

- IPEDS Completions Survey (1984-2023)
- IPEDS Directory Information (2011-2023)

!!!warning
    Race/ethnicity is unavailable for completions data from 1984-1994. All race/ethnicity columns have been set to "unknown" during this time period.

    In addition, race/ethnicity encoding changed between 2010 and 2011 data.

### Which survey variables are included in `scipeds`?

It depends. If the variable is part of one of the surveys currently included in the package, then yes. If it's part of a different survey that's not currently part of the package (e.g. Fall Enrollment), then no.

### `scipeds` doesn't have the data I need. What should I do?

We're always looking for contributors and would love for you to add it to the package! Check out the contributing guide to get started, or start a new [discussion](https://github.com/scienceforamerica/scipeds/discussions/new/choose) to share your idea and we'll be happy to work with you to make it happen!

## Completions

### What completions data is currently available?

`scipeds` uses data from the "A" series of aggregated IPEDS Completions Suvey data, which contains completers by university (6-digit UNITID), year, field of study (6-digit CIP code), award level, race/ethnicity, and gender. This data is available in the `ipeds_completions_a` table in the pre-processed duckdb.

In addition to the 6-digit CIP code, higher-level field taxonomies are also provided. The following is a complete schema for the `ipeds_completions_a` table:

<!-- COMPLETIONS TABLE SCHEMA -->

### The queries in the Engine don't cover my use case. What should I do?

You can write your own SQL query and run it using the engine's `get_df_from_query` function.

### I've written a super useful query that I think should be part of the engine. What should I do?

You can add a wrapper to your query in the engine definition. See the contributing guide for how to contribute to the codebase.

### How do query filters work?

Query filters allow you to specify the data you want to include for your analysis. 

For example, if you only want to look at Associate's degrees and want to exclude non-resident aliens from your analysis, you can specify the race/ethnicity groups in your `QueryFilter` as all groups except `RaceEthn.nonres` and the award levels as just `AwardLevel.associates`. By default, query filters will include all years of data, all race/ethnicity groups, both first and second majors, and all award levels.

!!!warning
    Excluding items from your queries will change the values you see in any `totals` columns. If you do filter your data, make sure you double-check or cross-reference the numbers you get back to make sure you are seeing what you would expect.

### I ran a query - what are the different columns and what do they mean?

It depends a little bit on the query, but in general each value returned by a completions query counts the number of awards or degrees received by members within a particular group. The number of awards is subject to any specified `QueryFilters`:

- `rollup_degrees_within_group` is the number of degrees awarded to a particular group (specified by your `grouping`) across all the fields specified in your `TaxonomyRollup`
- `rollup_degrees_total` is the number of degrees across all groups, across all the fields specified in your `TaxonomyRollup` 
- `field_degrees_within_group` is the number of degrees awarded to a particular group (specified by your `grouping`) within a field specified by a `FieldTaxonomy`
- `field_degrees_total` is the number of degrees within a specified field across all groups within a field specified by a `FieldTaxonomy`
- `uni_degrees_within_group` is the number of degrees awarded to a particular group, across all fields
- `uni_degrees_total` is the total number of degrees awarded across all groups and all fields

### What's the difference between a query that does something `by_grouping` as opposed to `within_grouping`?

Queries that aggregate data `by_grouping` will have columns containing `totals` across _all_ groupings. Queries that aggregate data `within_grouping` will always be indexed by the `intersectional` grouping, but the `total` column will be the number of degrees within that particular group. For example, aggregating within gender will lead to totals that correspond to the number of degrees awarded to all women rather than to all students.

### Why don't my sub-group totals add to the total value?

Queries generally return only non-zero values. If your query doesn't return a non-zero value for each group, then the sub-totals won't add up to the values in the `totals` columns.

### What is the deal with first and second majors?

IPEDS records students who [double major](https://surveys.nces.ed.gov/ipeds/public/survey-materials/instructions?instructionid=30080), and records one of these majors as secondary. By default, both first and second majors are included in aggregates (so one student may be counted more than once). You can aggregate only by first majors by choosing the appropriate `QueryFilter` values.

