# `scipeds` Contribution Guidelines

We welcome new contributions to `scipeds`! Our goal is to make this package a more useful resource for people working with IPEDS data, and we cannot accomplish that without support from contributors.

This guide will detail how you can contribute, whether you are requesting a new feature or adding functionality to `scipeds`.

!!! note

    We know that contributing to open-source projects can feel intimidating! We want contributing to be accessible and not scary, so if this guide feels overwhelming we encourage you to reach out by starting a new [discussion](https://github.com/scienceforamerica/scipeds/discussions/new/choose). We are humans on the other end, and will be excited to get back to you and figure out how we can help get you started.


## Code of conduct

All contributors must follow the [code of conduct](https://github.com/scienceforamerica/scipeds/main/CODE_OF_CONDUCT.md)

## Reporting a bug

To report a bug, file an [issue](https://github.com/scienceforamerica/scipeds/issues/new). Please report as much context about the issue as possible (e.g., the line of code where scipeds failed, the OS and python version you are using, any error message or traceback detailing the failure).

## Requesting new features

To request a new feature be added to `scipeds`, start a new [discussion](https://github.com/scienceforamerica/scipeds/discussions/new/choose).

## Developing new features

This section details how to develop new features for `scipeds`, with some additional information about code architecture and design considerations.

### Local development

To add new functionality to `scipeds`, it is useful to have the source code working on your own machine. This section contains details for how to get `scipeds` working locally.

 This project uses a `Makefile` to streamline development. To make these steps easier to run, make sure that `make` is installed on your system.

0. Clone the repo and create a new Python environment. If you have conda, you can use `make create_environment` - otherwise, use your virtual environment creation tool of choice.
0. Install the requirements (`make requirements`).
0. Run the tests (`make test`).
0. Build the documentation (`make docs`)
0. Serve the documentation locally (`make docs-serve`)
0. Download the data.
    - To download the pre-processed duckdb, run `scipeds download_db`.
    - If you want the raw data files. You can download them from Science for America's cloud storage (`make download-raw`) or directly from IPEDS (`make download-raw-from-ipeds`). You can then process the raw data files into a processed duckdb (`make process`).

### Architecture and design considerations

The `scipeds` repo actually contains two different things:

- an _application_ (in `pipeline/`) for downloading and processing raw data from IPEDS into a duckdb database
- a _library_ (in `scipeds/`) to make it easy to query IPEDS data from Python

Contributions to IPEDS will generally fall into two categories: adding new queries of existing IPEDS data (which currently contains just completions data and institutional directory information) and adding new data sources to the duckdb database.

### Adding new queries

The basis for `scipeds` queries is the `IPEDSQueryEngine`. This base class connects to the pre-processed duckdb and returns the results of various queries. Sets of queries for specific tables or purposes are factored out into their own sub-classes. For example, the `CompletionsQueryEngine` in `scipeds/data/completions.py`, offers a several built-in queries for aggregations that the authors used frequently to explore completions data. The universe of possible queries is quite large (which is why you can simply write your own SQL query using the `fetch_df_from_query` method of the `IPEDSQueryEngine`) but if that gets tiresome you might want to add a new function to an existing class or build a new class altogether.

The process for adding queries is relatively straightforward:

1. If you are creating a new query engine (for example because you are adding a new IPEDS survey component to `scipeds`), create a new file and create your class as a subclass of the `IPEDSQueryEngine`. If you are adding a query to an existing engine, add a new function for your query to the engine.
1. Create a template for your SQL query in the appropriate place. For example, for the completions queries in `CompletionsQueryEngine`, the longer queries are class attributes for neatness and shorter queries live with their corresponding functions. Queries can use a mix of [Python string formatting](https://docs.python.org/3/tutorial/inputoutput.html#formatted-string-literals) and (duckdb prepared statements)[https://duckdb.org/docs/sql/query_syntax/prepared_statements.html] to inject variables into the queries.
1. Write a test for your function that correctly asserts that the data returned by your query matches what you expect from running the query on the test database (generated by running `python pipeline/db.py write-test-db`). Your tests should be added in the `tests/` folder located most closely to your code changes (create one if it does not exist).

Where possible, please use the set of models, conventions, and options that exist for the current set of queries. For example, use the existing `QueryFilters` model (or extend it) to filter data by race/ethnicity, year, etc., and use the existing `FieldTaxonomy` columns and `TaxonomyRollup` model to aggregate across fields within a particular field taxonomy.

### Adding new sources of data

IPEDS has lots of different survey components, and we only include a small fraction of them here. We would very much like to expand the set of data that this package covers!

If you are interested in adding a new data source but aren't sure where to start or find the following instructions confusing, please reach out! We are more than happy to work with you through the process.

To add a new data source, please follow these instructions so the maintainers can easily reproduce your work:

1. Identify the data source you'd like to add and come up with an easily identifiable and SQL-table-friendly name to add to `scipeds/constants.py`. For example, the completions table is called `ipeds_completions_a` and the directory info table is called `ipeds_directory_info`.
1. Add code to `pipeline/download.py` that downloads your data directly from IPEDS to local disk. 1. Create a new script for your source of data in `pipeline/` that converts the data to a series of interim CSV files that can be read into duckdb. See existing completions (`pipeline/completions.py`) and directory info (`pipeline/institutions.py`) scripts for examples.
1. Add code to `pipeline/process.py` that reads your interim CSVs into a duckdb table. Try to minimize the size of your table where possible by using `ENUM`s or smaller data types (see completions and directory info processing for examples).
1. [Add queries](#adding-new-queries) for your new table.
1. Add code to `pipeline/db.py` for adding fake data for your new table to the test database.
1. Generate the new test database and add tests where appropriate for any new queries or other functionality.

!!! note

    By default, `scipeds` will use the database in the `SCIPEDS_CACHE_DIR` for query engines. As you are developing a new pipeline, `make process` outputs data to the interim and processed data directories in the `DATA_DIR`. Make sure you are querying the correct database.

### Updating data

IPEDS releases new data yearly. Package maintainers can follow these steps to update the data:

1. Download the data from IPEDS and upload it to SfA's Google Cloud `scipeds-data` bucket in the respective folder. Make sure to grab both the new year of data as well as the previous year's revised data.
1. Update the pipeline code that downloads the data. Make sure it uses the new year, and that both downloading directly from IPEDS (`make download-raw-from-ipeds`) and from Science for America's cloud storage (`make download-raw`) work.
1. Process the data into a the duckdb file (`make process`).
1. Do a bit of sanity-checking on the new duckdb file to make sure you don't have unexpected new or missing columns in the metadata, and that the total number of students seems okay.

!!! note

    If you manually upload the data into the storage bucket by first creating a folder called {year}, you might need to then delete that folder for the download code to work. (For some reason, Google Cloud creates a zero-byte object placeholder, which the download code sees as a file rather than a directory.)

    gsutil rm gs://scipeds-data/raw/ipeds_completions_a/{year}/
    gsutil rm gs://scipeds-data/raw/ipeds_directory_info/{year}/

To push these updates to production, simply release a new version of `scipeds`. The release actions automatically download all the data from cloud storage, process it, and upload the updated duckdb file to a public google cloud bucket.

## Submitting a pull request

1. Confirm that you are fixing an existing [issue](https://github.com/scienceforamerica/scipeds/issues/) or adding a [requested feature](https://github.com/scienceforamerica/scipeds/discussions). If an issue or discussion doesn't exist for your desired contribution, create it!
1. Fork the repo, clone it locally, and follow the instructions for [local development](#local-development)
1. Run formatting (`make format`) and linting (`make lint`).
1. Submit your PR!