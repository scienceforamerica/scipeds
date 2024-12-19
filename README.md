# `scipeds`

A Python package for working with [IPEDS](https://nces.ed.gov/ipeds/) [data](https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx).

Read the full documentation [here](https://scipeds.onrender.com/).

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

## Quickstart

### Install `scipeds`

```bash
# with pip
pip install ipeds
```

### Download the pre-processed database

From the shell: 

```bash
scipeds download-db
```

or from python / a notebook: 

```python
import scipeds

scipeds.download_db()
```

### Query completions data using the corresponding query engine

```python
from scipeds.data.completions import CompletionsQueryEngine
from scipeds.data.queries import (
    FieldTaxonomy,
    QueryFilters, 
)

engine = CompletionsQueryEngine()
```

Use a pre-baked query:
```python
gender_df = engine.field_totals_by_grouping(
    grouping="gender", 
    taxonomy=FieldTaxonomy.ncses_field_group
)
```

or write your own using [duckdb SQL syntax](https://duckdb.org/docs/sql/introduction.html):

```python
from scipeds.constants import COMPLETIONS_TABLE

df = engine.get_df_from_query(f"""
    SELECT * 
    FROM {COMPLETIONS_TABLE}
    LIMIT 10;
""")
```

For more detailed usage, see the [Usage page](https://scipeds.onrender.com/usage) or the [engine API Reference](https://scipeds.onrender.com/data).

## About `scipeds`

### What is `scipeds`?

`scipeds` is a Python package for working with data from IPEDS. Specifically, `scipeds` makes it easier for people to analyze data from IPEDS by pre-processing and standardizing IPEDS data into a database and providing some Python tooling for querying that database.

`scipeds` is _not_ a tool for working with raw IPEDS data. For that, you should download data directly from [IPEDS](https://nces.ed.gov/ipeds/). 

Full `scipeds` documentation can be found at [this link](https://scipeds.onrender.com/), and the source code is avilable on [GitHub](https://github.com/scienceforamerica/scipeds).

#### Currently supported IPEDS surveys

`scipeds` currently supports the following datasets / survey components:

- IPEDS Completions by program (6-digit CIP code), award level, race/ethnicity, and gender from 1995-2023
- IPEDS Institutional Characteristics Directory Information from 2011-2023

#### Completions data preprocessing

We provide functionality to reproduce our pre-processing of the IPEDS data. To recreate the pre-processed database, you can clone the `scipeds` [repository](https://github.com/scienceforamerica/scipeds), download the raw data, and re-run the pipeline code in `pipeline/`. Decisions about how to convert / crosswalk data across different years and handle other edge cases such as missing data are contained in the pipeline code.

### Why does `scipeds` exist?

While IPEDS provides a large volume of data about higher education in the United States, working with IPEDS data can be challenging! Many things have changed in the time that data has been reported to IPEDS, making it non-trivial to join datasets across different time periods to consistently measure changes over time. 

In the process of their own work, the authors found it useful to create tools to make it easier to analyze IPEDS data and hoped that the tools they created would be useful to others as well.

### Who created `scipeds`?

`scipeds` was created by [Science for America](https://www.scienceforamerica.org/) as part of its mission to address urgent challenges in STEM education.