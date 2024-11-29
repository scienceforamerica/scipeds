# Pipeline assets

The following is a description of each of the source assets used for the purposes of classifying different disciplines into field taxonomies:

* `DHS_STEM_2024.pdf` comes from the [Department of Homeland Security](https://www.ice.gov/doclib/sevis/pdf/stemList2024.pdf) and it contains:
    >  a complete list of fields of study that DHS considers to be science, technology, engineering or mathematics (STEM)fields of study for purposes of the 24-month STEM optional practical training extension described at _8 CFR 214.2(f)_.
* `dhs_stem_classification_table.csv` is a manual enumeration of the CIP codes in `DHS_STEM_2024.pdf`
* `ncses_stem_classification_table.csv` is an export of data from the [NCSES Data Portal](https://ncsesdata.nsf.gov/builder/ipeds_c) using "Fields of Study" >> "Academic Discipline (Alternative Classification)" including all nested values as the rows of the table and "Degrees Awarded by Colleges and Universities" >> "Awards/Degrees Conferred" as the measure of the table

Note that the values in the NCSES table are different than the values you'll get from querying the duckdb. This is because the NCSES table uses only first majors and doesn't use the revised numbers published each year.