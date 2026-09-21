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

Needs credentials. If `gsutil` says they're invalid, the user runs `gcloud auth login`
themselves — suggest they type `! gcloud auth login` in the prompt.

```bash
gsutil -m cp -r data/raw/ipeds_completions_a/{year} \
  gs://scipeds-data/raw/ipeds_completions_a/{year}/
gsutil -m cp -r data/raw/ipeds_directory_info/{year} \
  gs://scipeds-data/raw/ipeds_directory_info/{year}/
```

Upload the **revised prior year** too — that's a change to already-uploaded data, so it
needs overwriting, not just adding.

### The zero-byte folder gotcha

If a folder was created by hand in the Cloud Console, Google leaves a zero-byte
placeholder object behind. The download code sees it as a file rather than a directory
and breaks:

```bash
gsutil rm gs://scipeds-data/raw/ipeds_completions_a/{year}/
gsutil rm gs://scipeds-data/raw/ipeds_directory_info/{year}/
```

### Verify the round trip

Before releasing, confirm the bucket actually has what the workflow will need:

```bash
make download-raw   # into a scratch DATA_PATH, not over your working data
```

This is the real test that the release will work.

## Phase 10 — Release

Follow `RELEASING.md`. In short:

1. All PRs merged to `main`.
2. `pyproject.toml` version is the real release version, matching what you'll type into
   the workflow — the workflow derives the duckdb filename from its input and fails the
   existence check if they disagree.
3. `HISTORY.md` updated, with the `Unreleased` link and the new release link at the
   bottom.
4. GitHub → Actions → `release` → "Run workflow" → enter the version (e.g. `v0.0.9`).

The workflow downloads raw data from GCS, reprocesses it, uploads the built duckdb to
`gs://scipeds-data/processed/`, builds the package, publishes to Test PyPI and PyPI, and
creates the GitHub release.

Documentation redeploys automatically from Render when PRs merge.
