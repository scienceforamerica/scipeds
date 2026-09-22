# Uploading raw data and cutting a release

**Everything here is gated.** These steps push data to Google Cloud and publish to PyPI.
Only run them when the user explicitly asks, after the data update PR has been reviewed
and merged.

## Why the raw data has to go to GCS

The release workflow (`.github/workflows/release.yml`) does not download from IPEDS. It
runs `make download-raw`, which pulls everything from `gs://scipeds-data/raw/`. If the new
year isn't in the bucket, the release will quietly build a database without it.

This is also why the bucket is the backup of record: IPEDS URLs move and older years drop
off the newer host, but the bucket keeps every year we've ever ingested.

## Phase 9 — Upload raw data

Two different logins, both run by the user (suggest they type `! <command>`):

- `gcloud auth login` — for `gsutil`, i.e. the uploads below.
- `gcloud auth application-default login` — for the round-trip check, which goes through
  cloudpathlib and reads application-default credentials instead. Having one does not
  give you the other.

```bash
gsutil -m cp data/raw/ipeds_completions_a/{year}/*  gs://scipeds-data/raw/ipeds_completions_a/{year}/
gsutil -m cp data/raw/ipeds_directory_info/{year}/* gs://scipeds-data/raw/ipeds_directory_info/{year}/
```

Upload the **revised prior year** too — that's a change to already-uploaded data, so it
needs overwriting, not just adding.

The new year's completions data dictionary won't be on disk: `download-from-ipeds
--survey-year` deliberately skips it. Nothing in the pipeline reads it, but every other
year has one, so fetch it and upload it alongside:

```bash
curl -sSLO https://nces.ed.gov/ipeds/complete-data-files/C{year}_A_Dict.zip
gsutil cp C{year}_A_Dict.zip gs://scipeds-data/raw/ipeds_completions_a/{year}/
```

### Check the folders after uploading

Uploading does not replace a file unless the name matches exactly, and IPEDS changes the
casing of its filenames between years (`HD2024.csv` one year, `hd2024.csv` the next). So
a year that already existed in the bucket can end up with two copies of the same data
under two spellings. List each folder and look:

```bash
gsutil ls -l gs://scipeds-data/raw/ipeds_completions_a/{year}/
gsutil ls -l gs://scipeds-data/raw/ipeds_directory_info/{year}/
```

Each `ipeds_directory_info/{year}/` must contain **exactly one csv and exactly one Excel
file** — `institutions.py` raises `FileNotFoundError` otherwise, which is how the 2025
update would have failed mid-release. Each completions folder should have the `_rv` file
for any year that has one, and no leftover copy of the same csv under a different case.

If there's a leftover, archive it before removing it. Bucket versioning is **suspended**,
so a delete is permanent:

```bash
gsutil cp gs://scipeds-data/raw/<path>/<file> gs://scipeds-data/superseded/{year}/<file>
gsutil rm gs://scipeds-data/raw/<path>/<file>
```

The archive goes in `superseded/`, not under `raw/`. `download-from-bucket` mirrors the
whole `raw/` prefix, so anything parked there gets pulled into every build.

### The zero-byte folder gotcha

If a folder was created by hand in the Cloud Console, Google leaves a zero-byte
placeholder object behind. The download code sees it as a file rather than a directory
and breaks:

```bash
gsutil rm gs://scipeds-data/raw/ipeds_completions_a/{year}/
gsutil rm gs://scipeds-data/raw/ipeds_directory_info/{year}/
```

### Verify the round trip

Downloading isn't enough — build from what came back, since that's what the release does:

```bash
export DATA_PATH=/tmp/roundtrip          # never over your working data
python pipeline/download.py download-from-bucket --data-dir $DATA_PATH/raw
make process
```

Then compare the result against the database you built locally: same number of years,
same award totals per year, same institution count. Differing file sizes are fine —
duckdb layout isn't byte-stable — but the numbers have to match.

## Phase 10 — Release

Follow `RELEASING.md`. In short:

1. All PRs merged to `main`.
2. `pyproject.toml` version is the real release version, matching what you'll type into
   the workflow — the workflow derives the duckdb filename from its input and fails the
   existence check if they disagree.
3. `HISTORY.md` updated: the `[Unreleased]` heading becomes `## [vX.Y.Z] (date)`, and
   the links at the bottom get the new release. The heading is load-bearing — the
   workflow pulls the release notes out of `HISTORY.md` by matching the version string
   you type in, so leaving it as `[Unreleased]` publishes a release with an empty body.
4. GitHub → Actions → `release` → "Run workflow" → enter the version (e.g. `v0.0.9`).

The workflow downloads raw data from GCS, reprocesses it, uploads the built duckdb to
`gs://scipeds-data/processed/`, builds the package, publishes to Test PyPI and PyPI, and
creates the GitHub release.

Documentation redeploys automatically from Render when PRs merge.
