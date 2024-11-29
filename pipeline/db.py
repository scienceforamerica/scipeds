import tempfile
from pathlib import Path

import duckdb
import pandas as pd
import typer

import pipeline.settings
from pipeline.settings import logger
from scipeds import constants
from scipeds.data.enums import (
    AwardLevel,
    Gender,
    NCSESDetailedFieldGroup,
    NCSESFieldGroup,
    NCSESSciGroup,
    NSFBroadField,
    RaceEthn,
)

app = typer.Typer()


def write_completions_to_db(
    con: duckdb.DuckDBPyConnection, completions_dir: Path, verbose: bool = True
):
    """Read IPEDS data from interim CSVs into structured duckdb table"""
    enum_cols = [
        "unitid",
        "awlevel",
        "race_ethnicity",
        "gender",
        "cip_title",
        "ncses_sci_group",
        "ncses_field_group",
        "ncses_detailed_field_group",
        "nsf_broad_field",
    ]
    combined_enum_cols = {"cip_enum": ("cipcode", "cip2020")}

    descriptions = {
        "year": "IPEDS survey year",
        "unitid": "UNITID of institution",
        "cipcode": "The originally recorded CIP code",
        "awlevel": "Level of award",
        "majornum": "Major number",
        "cip2020": "Crosswalked 2020 CIP code",
        "race_ethnicity": "Race / ethnicity of completers",
        "gender": "Gender of completers",
        "n_awards": "Number of completions",
        "ncses_sci_group": "NCSES Science and Engineering Alternate Classification",
        "ncses_field_group": "NCSES Broad Fields Alternate Classification",
        "ncses_detailed_field_group": "NCSES Detailed Fields Classification",
        "nsf_broad_field": "NSF Diversity and STEM Report Broad Field Classification",
        "dhs_stem": "DHS STEM Classification",
    }

    create_query = f"""
        CREATE TABLE {constants.COMPLETIONS_TABLE} AS (
            SELECT * FROM read_csv('{str(completions_dir.resolve())}/*.csv.gz',
                delim = ',',
                header = true,
                auto_detect = false,
                columns = {{
                    'year': 'USMALLINT',
                    'unitid': 'VARCHAR',
                    'cipcode': 'VARCHAR',
                    'awlevel': 'VARCHAR',
                    'majornum': 'UTINYINT',
                    'cip2020': 'VARCHAR',
                    'race_ethnicity': 'VARCHAR',
                    'gender': 'VARCHAR',
                    'n_awards': 'UINTEGER',
                    'cip_title': 'VARCHAR',
                    'ncses_sci_group': 'VARCHAR',
                    'ncses_field_group': 'VARCHAR',
                    'ncses_detailed_field_group': 'VARCHAR',
                    'nsf_broad_field': 'VARCHAR',
                    'dhs_stem': 'BOOLEAN'
                }})
        );
    """
    if verbose:
        logger.info(f"Reading interim CSVs into table {constants.COMPLETIONS_TABLE}")
    con.execute(create_query)

    # Convert enumerated columns to enum type
    for col in enum_cols:
        query = f"SELECT COUNT(DISTINCT {col}) FROM {constants.COMPLETIONS_TABLE}"
        num_values = con.sql(query).df().values[0]
        if verbose:
            logger.info(f"Creating enum {col}_enum with {num_values} unique values...")
        create_enum_stmt = f"""
            CREATE TYPE {col}_enum AS ENUM
                (SELECT DISTINCT {col} FROM {constants.COMPLETIONS_TABLE} ORDER BY {col})
            """
        con.execute(create_enum_stmt)
        alter_stmt = f"ALTER TABLE {constants.COMPLETIONS_TABLE} ALTER {col} TYPE {col}_enum"
        con.execute(alter_stmt)

    # Create enumerated columns where we want the enumerated values
    # to span multiple "original" columns
    for enum_name, cols in combined_enum_cols.items():
        subqueries = [f"SELECT DISTINCT {col} FROM {constants.COMPLETIONS_TABLE}" for col in cols]
        subquery = " UNION ".join(subqueries)
        query = f"SELECT DISTINCT(*) AS {enum_name} FROM ({subquery}) ORDER BY {enum_name}"
        num_values = con.sql(query).df().values[0]
        if verbose:
            logger.info(f"Creating enum {enum_name} with {num_values} values from cols {cols}")
        create_enum_stmt = f"CREATE TYPE {enum_name} AS ENUM ({query})"
        con.execute(create_enum_stmt)
        for col in cols:
            alter_stmt = f"ALTER TABLE {constants.COMPLETIONS_TABLE} ALTER {col} TYPE {enum_name}"
            con.execute(alter_stmt)

    # Add column descriptions
    for col, desc in descriptions.items():
        con.execute(f"COMMENT ON COLUMN {constants.COMPLETIONS_TABLE}.{col} IS '{desc}'")

    if verbose:
        n_rows = con.sql(f"SELECT COUNT(*) FROM {constants.COMPLETIONS_TABLE}").df().values[0]
        logger.info(f"Created table {constants.COMPLETIONS_TABLE} with {n_rows[0]:,} rows.")
        logger.info(con.sql(f"DESCRIBE {constants.COMPLETIONS_TABLE}"))
        logger.info("Creating table with CIP codes...")

    # Create CIP table
    con.sql(f"""CREATE TABLE {constants.CIP_TABLE} AS 
    (
        SELECT DISTINCT ON (cip2020)
            cip2020,
            cip_title,
            ncses_sci_group,
            ncses_field_group,
            ncses_detailed_field_group,
            nsf_broad_field,
            dhs_stem
        FROM {constants.COMPLETIONS_TABLE}
    );
    """)

    con.sql(f"ALTER TABLE {constants.COMPLETIONS_TABLE} DROP COLUMN cip_title")

    n_rows = con.sql(f"SELECT COUNT(*) FROM {constants.CIP_TABLE}").df().values[0]
    if verbose:
        logger.info(f"Created table {constants.CIP_TABLE} with {n_rows[0]:,} rows.")
        logger.info(con.sql(f"DESCRIBE {constants.CIP_TABLE}"))


def write_institutions_to_db(con: duckdb.DuckDBPyConnection, dir: Path, verbose: bool = True):
    """Read institution metadata CSV to a table"""
    create_query = f"""
        CREATE OR REPLACE TABLE {constants.INSTITUTIONS_TABLE} AS (
            SELECT * FROM read_csv('{str(dir.resolve())}/*.csv.gz',
                delim = ',',
                header = true,
                all_varchar = true
            )
        );
    """
    con.execute(create_query)
    if verbose:
        n_rows = con.sql(f"SELECT COUNT(*) FROM {constants.INSTITUTIONS_TABLE}").fetchall()[0][0]
        logger.info(f"Created table institution meta with {n_rows:,} rows.")
        logger.info(con.sql(f"DESCRIBE {constants.INSTITUTIONS_TABLE}"))


@app.command()
def write_db(
    output_file: Path = pipeline.settings.PROCESSED_DATA_DIR / constants.DB_NAME,
    completions_dir: Path = pipeline.settings.INTERIM_DATA_DIR / constants.COMPLETIONS_TABLE,
    institutions_dir: Path = pipeline.settings.INTERIM_DATA_DIR / constants.INSTITUTIONS_TABLE,
    overwrite: bool = False,
    verbose: bool = True,
):
    """Create a duckdb database"""
    if output_file.exists():
        if not overwrite:
            raise FileExistsError(
                f"File {output_file} exists and overwrite is false - "
                "delete file or set overwrite to true"
            )
        else:
            output_file.unlink()
    output_file.parents[0].mkdir(exist_ok=True, parents=True)
    con_str = str(output_file.resolve())
    con = duckdb.connect(con_str)
    write_completions_to_db(con, completions_dir, verbose=verbose)
    write_institutions_to_db(con, institutions_dir, verbose=verbose)
    con.close()


def create_test_record(race_ethnicity: RaceEthn, gender: Gender, n_awards: int, **kwargs):
    """Create a completions db record for testing purposes"""
    record = dict(
        year=kwargs.get("year", constants.END_YEAR),
        unitid=kwargs.get("unitid", "1"),
        cipcode=kwargs.get("cipcode", "00.0000"),
        awlevel=kwargs.get("award_level", AwardLevel.bachelors.value),
        majornum=kwargs.get("majornum", 1),
        cip2020=kwargs.get("cipcode", "11.1111"),
        race_ethnicity=race_ethnicity.value,
        gender=gender.value,
        n_awards=n_awards,
        cip_title=kwargs.get("cip_title", "Field title"),
        ncses_sci_group=kwargs.get("ncses_sci_group", NCSESSciGroup.sci.value),
        ncses_field_group=kwargs.get("ncses_field_group", NCSESFieldGroup.math_cs.value),
        ncses_detailed_field_group=kwargs.get(
            "ncses_detailed_field_group", NCSESDetailedFieldGroup.math.value
        ),
        nsf_broad_field=kwargs.get("nsf_broad_field", NSFBroadField.math_cs.value),
        dhs_stem=kwargs.get("dhs_stem", True),
    )
    return record


def fake_institution_data() -> pd.DataFrame:
    """Construct fake institution data for testing purposes"""
    data = [{"unitid": "1", "name": "University 1"}, {"unitid": "2", "name": "University 2"}]
    meta = pd.DataFrame.from_records(data)
    return meta


def fake_completions_data() -> pd.DataFrame:
    """Construct fake data dataframe with fixed values for query testing.

    Details about this fake world / dataframe:
    - two universities during one year for bachelors
    - two genders (male, female)
    - three races (black, asian, hispanic)
    - three majors (math, psych, eng_lit)a

    Some things to make testing easier:
    - assume 1 person in every sub-bucket

    """
    unitids = ["1", "2"]
    years = [constants.START_YEAR, constants.END_YEAR]
    genders = [Gender.women, Gender.men]
    race_ethns = [RaceEthn.black_or_aa, RaceEthn.asian, RaceEthn.hispanic]
    sgs = [NCSESSciGroup.sci, NCSESSciGroup.non_sci, NCSESSciGroup.unknown]
    fgs = [NCSESFieldGroup.math_cs, NCSESFieldGroup.psych, NCSESFieldGroup.humanities]
    dfgs = [
        NCSESDetailedFieldGroup.math,
        NCSESDetailedFieldGroup.psych,
        NCSESDetailedFieldGroup.english_lit,
    ]
    broad_fields = [NSFBroadField.math_cs, NSFBroadField.soc_behav_sci, NSFBroadField.non_stem]
    cips = ["27.0101", "42.2706", "23.0801"]
    titles = [
        "Mathematics, General",
        "Behavioral Neuroscience",
        "English Literature (British And Commonwealth)",
    ]

    rows = []
    for unitid in unitids:
        for year in years:
            for title, sg, fg, dfg, bf, cip in zip(titles, sgs, fgs, dfgs, broad_fields, cips):
                for race_ethn in race_ethns:
                    for gender in genders:
                        count = 1
                        row = create_test_record(
                            year=year,
                            unitid=unitid,
                            cip_title=title,
                            ncses_sci_group=sg.value,
                            ncses_field_group=fg.value,
                            ncses_detailed_field_group=dfg.value,
                            nsf_broad_field=bf.value,
                            cipcode=cip,
                            race_ethnicity=race_ethn,
                            gender=gender,
                            n_awards=count,
                        )
                        rows.append(row)
    return pd.DataFrame(rows)


@app.command()
def write_test_db(output_dir: Path = pipeline.settings.LIBRARY_ROOT / "data" / "tests" / "assets"):
    """Create a duckdb for testing purposes"""
    # We want to replicate the process for creating the duckdb as closely as we can
    # We do this by dumping our fake dataset to a csv and read it into a duckdb
    # using `write_completions_to_db` to a temporary db file
    temp_dir = tempfile.TemporaryDirectory()

    # Generate temporary intermediate CSVs for completions
    completions_dir = tempfile.TemporaryDirectory(dir=Path(temp_dir.name))
    completions_csv = tempfile.NamedTemporaryFile(dir=Path(completions_dir.name), suffix=".csv.gz")
    completions_df = fake_completions_data()
    completions_df.to_csv(completions_csv.name, index=False, compression="gzip")

    # Generate temporary intermediate CSVs for institutions
    institutions_dir = tempfile.TemporaryDirectory(dir=Path(temp_dir.name))
    institutions_csv = tempfile.NamedTemporaryFile(
        dir=Path(institutions_dir.name), suffix=".csv.gz"
    )
    institutions_df = fake_institution_data()
    institutions_df.to_csv(institutions_csv.name, index=False, compression="gzip")

    # Write everything to CSV
    temp_db = output_dir / "test.duckdb"
    if temp_db.exists():
        temp_db.unlink()
    con = duckdb.connect(str(temp_db))
    write_completions_to_db(con, Path(completions_dir.name))
    write_institutions_to_db(con, Path(institutions_dir.name))
    con.close()

    completions_csv.close()
    institutions_csv.close()
    completions_dir.cleanup()
    institutions_dir.cleanup()
    temp_dir.cleanup()


if __name__ == "__main__":
    app()
