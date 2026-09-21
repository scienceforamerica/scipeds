---
name: update-ipeds-data
description: Add a new year of IPEDS data to scipeds. Use when adding/updating IPEDS completions or institution directory data, bumping END_YEAR, or when the user mentions a new data year (e.g. "add 2026 data"), refreshing raw data from IPEDS, or reviewing what a data rebuild changed.
---

# Updating IPEDS data in scipeds

IPEDS publishes a new year of Completions (`C{year}_A`) and Institution Directory
(`HD{year}`) data each year, and revises the previous year at the same time. This skill
walks through absorbing a new year.

**The work is mostly mechanical. The risk is not.** Every year something small changes on
the IPEDS side — a moved URL, re-cased data dictionary sheet names, a BOM in a CSV header,
a dropped column — and none of it raises an error. It shows up as quietly wrong data. So
the steps below front-load *looking at the raw files* before processing, and end with a
mandatory human review of what actually changed.

Work through the phases in order. **Stop at each checkpoint.** Do not chain phases
together in one go.

---

## Phase 1 — Preflight

1. Confirm a clean working tree, then branch: `git checkout -b add-{year}-data`.
2. **Find out where the data actually is.** IPEDS serves files from more than one URL and
   the split changes. As of 2026-09: `https://nces.ed.gov/ipeds/complete-data-files/`
   has only 2023–2025; `https://nces.ed.gov/ipeds/datacenter/data/` has 1984–2024 but
   its recent-year copies are the *provisional* versions. Check both for the new year and
   the prior year rather than assuming last year's mapping holds:

   ```bash
   for base in https://nces.ed.gov/ipeds/complete-data-files \
               https://nces.ed.gov/ipeds/datacenter/data; do
     for f in C{year}_A.zip C{year}_A_Dict.zip HD{year}.zip HD{year}_Dict.zip \
              C{prev}_A.zip HD{prev}.zip; do
       printf "%-18s %-45s " "$f" "$(basename $base)"
       curl -sIL "$base/$f" | grep -iE '^(HTTP/|content-length|last-modified)' \
         | tr -d '\r' | tr '\n' ' '; echo
     done
   done
   ```

   A `200` with a recent `last-modified` is what you want. Compare sizes and dates
   between the two URLs for the prior year — a much larger file usually means it now
   contains the revised (`_rv`) data.

**Checkpoint:** report which URL serves which files, and whether the prior year's revised
file has appeared. Wait for the go-ahead.

---

## Phase 2 — Point the code at the new year

1. `scipeds/constants.py`: bump `END_YEAR`.
2. `pipeline/download.py`: update `COMPLETE_DATA_FILES_URL_YEARS` to match what Phase 1
   found. This set is the years fetched from `COMPLETE_DATA_FILES_URL` instead of
   `DATACENTER_URL`; everything else uses `DATACENTER_URL`.
3. Print the generated URLs and eyeball them:

   ```bash
   python -c "
   from pipeline.download import COMPLETION_ZIP_FILENAMES as C, INSTITUTION_METADATA_FILENAMES as H
   for y in sorted(C)[-4:]: print(y, C[y])
   for y in sorted(H)[-4:]: print(y, H[y])
   "
   ```

---

## Phase 3 — Download

`download_and_extract` skips anything already on disk, so **clear the year directories
first** or you'll silently keep stale files. Move rather than delete, in case you need to
compare:

```bash
for d in data/raw/ipeds_completions_a/{year} data/raw/ipeds_directory_info/{year} \
         data/raw/ipeds_completions_a/{prev} data/raw/ipeds_directory_info/{prev}; do
  [ -d "$d" ] && mv "$d" /tmp/raw-backup/"$d"
done
python pipeline/download.py download-from-ipeds --survey-year {year}
python pipeline/download.py download-from-ipeds --survey-year {prev}
```

Re-downloading the prior year is what picks up its revised data — IPEDS republishes the
same zip with an extra `_rv` CSV inside.

Note on macOS: the filesystem is case-insensitive, so an old `C2024_a.csv` and a new
`c2024_a.csv` collide. Clearing the directory avoids the ambiguity.

---

## Phase 4 — Look at the raw files before processing

**Do not skip this.** This is the step that catches IPEDS format changes while they're
still cheap to fix. Run these and report what you find:

```bash
ls -la data/raw/ipeds_completions_a/{year}/ data/raw/ipeds_directory_info/{year}/
```

- **Is there an `_rv` file for the prior year?** If not, either it hasn't been published
  yet or you downloaded from the wrong URL.
- **Completions columns** — compare the new year's CSV header against the prior year's.
  Added or removed columns mean `pipeline/completions.py` needs attention (the race/gender
  layout is picked by inspecting column names, so it adapts on its own *unless* IPEDS
  renames things).
- **Award levels** — `sorted(df["AWLEVEL"].unique())`. A new code needs adding to
  `AWARD_LEVEL_CODES` in `pipeline/completions.py`.
- **HD data dictionary** — open `hd{year}.xlsx` and check the sheet names and their
  column names. In 2024 IPEDS changed the casing (`varTitle` → `vartitle`,
  `Frequencies` → `frequencies`), which broke processing. The reader now lowercases both,
  but confirm the sheets are still called something like `Varlist` and `Frequencies`.
- **HD CSV header** — check for a BOM (`\xef\xbb\xbf`) and for added/removed columns
  versus the prior year.

If a file looks like a revision but you're not sure, **diff it** rather than assuming.
Size changes are often just whitespace padding:

```bash
# after extracting both versions
diff <(sed 's/\r$//; s/[[:space:]]*$//' old.csv) \
     <(sed 's/\r$//; s/[[:space:]]*$//' new.csv) | head
```

**Checkpoint:** report what changed in the raw files. Wait before processing.

---

## Phase 5 — Process

`DB_NAME` comes from the *installed* package version, not `pyproject.toml`. Bump to a dev
version and reinstall so the build doesn't overwrite a real release:

```bash
# pyproject.toml: version = "0.0.9.dev0"
pip install -e . --no-deps
python -c "from scipeds import constants; print(constants.DB_NAME, constants.END_YEAR)"
make process
```

This takes several minutes. Watch for `There were N unclassified CIP codes in {year}` in
the log — Phase 6 checks that properly.

---

## Phase 6 — Review the changes (human decides)

```bash
make review-changes
```

This compares the new local build against the last released database (both local —
nothing has been pushed) and writes an HTML report.

The review script is a maintainer tool, not part of the pipeline, so its plotting
dependency lives in `requirements/review.txt` rather than `pipeline/requirements.in`.
Without matplotlib installed every check still runs and the report is still written —
just without charts. `pip install -r requirements/review.txt` to get them. Every check is PASS, NEEDS REVIEW, or
FAIL; only FAIL exits nonzero.

**Present the report and walk the user through each NEEDS REVIEW item in plain language**
— what changed, what you think, and why. Typical items:

- **Newly-unclassified CIP codes.** IPEDS added codes with no NCSES science
  classification. Decide: add them to the NCSES table in `pipeline/assets/`, or accept
  them as unclassified. The report ranks them by how many students they cover.
- **Institution columns added.** Usually new IPEDS variables and fine. Look at what
  showed up.
- **Columns dropped / unexpected year deltas.** These are FAILs; something needs fixing.

**This is a hard stop.** Do not continue to Phase 7 until the user explicitly says to —
not on an all-green report, not on a clean exit code. No FAILs means nothing obviously
broke, not that the data is right.

---

## Phase 7 — Repo updates

1. `pyproject.toml`: set the real release version (drop `.dev0`), then `pip install -e . --no-deps`.
2. `make test-assets` — `END_YEAR` is baked into the test fixture.
3. `HISTORY.md`: changelog entry under `[Unreleased]`.
4. `README.md`: update the "new data" note.
5. `make lint && make test && make test-pipeline`.

---

## Phase 8 — Pull request

Open a PR describing: the new year, the prior year's revision, any URL changes, and the
NEEDS REVIEW items the user accepted (especially newly-unclassified CIP codes — that's a
deliberate decision worth recording).

---

## Phases 9–10 — Cloud upload and release

**Gated.** These push data and artifacts to Google Cloud and PyPI. Never run them without
the user explicitly asking, and never as a continuation of Phase 8.

See `references/release.md`.
