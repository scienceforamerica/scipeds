# Steps to issue a new release

Before starting the release process:

- Merge all PRs to `main`
- Make sure you've updated the version number in TBD
- Update the changelog in `HISTORY.md` (manually)
    - Go to `HISTORY.md` and click on "unreleased" to view all the PRs you've merged since the last release
    - Add summary of the things you've added or changed
    - Update the `Unreleased` link and add the current release (at the bottom of `HISTORY.md`)

Now you are ready to issue the new release:

- Go to the "Actions" tab on the repo
- Click on the `release` workflow
- Click on "Run workflow" in the top right
- Enter the version you are releasing, click "Run"

What gets automatically updated through the release workflow:
- Downloading the raw data, reprocessing it, and uploading the duckdb file to GCS with the right version
- Build the package and upload it to PyPI
- Create a new release tag on GitHub

The documentation is automatically updated every time we merge a new PR via this project's autodeploy settings on Render.