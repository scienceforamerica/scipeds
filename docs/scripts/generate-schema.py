import duckdb
import pipeline.settings

from scipeds import constants

REPLACE_PATTERNS = {"<!-- COMPLETIONS TABLE SCHEMA -->": constants.COMPLETIONS_TABLE}


def table_schema(table: str):
    """Use the test-asset version of the duckdb to generate a schema"""
    test_db = pipeline.settings.LIBRARY_ROOT / "data" / "tests" / "assets" / "test.duckdb"
    con = duckdb.connect(str(test_db))
    schema = con.sql(f"""
                     SELECT column_name, comment, data_type 
                     FROM duckdb_columns()
                     WHERE table_name = '{table}'
    """).df()

    # Clean up enums
    schema.data_type = schema.data_type.str.replace(r"ENUM\(.*", "ENUM", regex=True)

    # Clean up columns

    # Include the table name as a section header
    result = f"#### {table}\n" + schema.to_markdown(index=False)
    return result


if __name__ == "__main__":
    print(f"Replacing {len(REPLACE_PATTERNS)} items.")
    for replace_pattern, table in REPLACE_PATTERNS.items():
        print(f"Replacing {replace_pattern} with: \n{table_schema(constants.COMPLETIONS_TABLE)}")

else:
    import mkdocs_gen_files

    with mkdocs_gen_files.open("faq.md", "r") as f:
        schema_file = f.read()

    for pattern, table in REPLACE_PATTERNS.items():
        schema_file = schema_file.replace(pattern, table_schema(table))

    with mkdocs_gen_files.open("faq.md", "w") as f:
        f.write(schema_file)
