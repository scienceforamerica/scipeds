# ![scipeds logo](scipeds.png)

A Python package for working with [IPEDS](https://nces.ed.gov/ipeds/) [data](https://nces.ed.gov/ipeds/datacenter/DataFiles.aspx).

---
## Selecting Columns
To limit the size of the returned table, you can specify which columns to load:

```python
institutions_df = engine.get_institutions_table(
    cols=["institution_name", "state_abbr", "control"]
)
```
- [Institution Characteristics](institutions.md)


--8<-- "README.md:6:"