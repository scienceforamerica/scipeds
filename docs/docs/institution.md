# Accessing Institution-Level Data

You can load institution-level data using the `get_institutions_table()` method from the `Engine` class.

This table includes one row per institution, with characteristics such as:

- `unitid`: Unique identifier for the institution
- `institution_name`: Full name of the institution
- `state_abbr`: Two-letter state code
- `control`: Institution type (public/private)
- `hbcu`: Flag for Historically Black Colleges and Universities
- `locale`: Classification of urbanicity

## Example

```python
from scipeds import Engine
```
engine = Engine()

# Load full institution dataset
institutions_df = engine.get_institutions_table()

# Preview first few rows
institutions_df.head()

---
### Selecting Columns
To limit the size of the returned table, you can specify which columns to load:
```python
institutions_df = engine.get_institutions_table(
    cols=["institution_name", "state_abbr", "control"]
)
```