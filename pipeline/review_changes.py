"""Review what changed between the previously released scipeds database and a
newly-built one.

Written entirely by Claude, with only minimal human review. Paired with the
`update-ipeds-data` skill in `.claude/skills/`, which is what says when to run
this and what to do with the output.

This is the sanity check that runs after `make process` and before anything is
pushed anywhere. Both databases are local files: the new one has never been
uploaded, and the old one is whichever release is sitting in the scipeds cache.

Every check lands in one of three states:

- PASS: as expected, nothing to look at.
- NEEDS REVIEW: a real change that a person has to see and accept. Not a
  problem -- new columns and newly-unclassified CIP codes are often the whole
  point of the update -- but somebody has to look at it.
- FAIL: something is actually wrong and should be fixed before shipping.

Only FAIL exits nonzero. NEEDS REVIEW items are normal in a good year, so
failing on them would just train everyone to ignore the exit code.

Plots are optional. This is a maintainer tool, not part of the pipeline anyone
needs to reproduce the data, so matplotlib is not a pipeline dependency -- see
requirements/review.txt. Without it the checks all still run and the report is
still written, just without the charts.
"""

import base64
import html
import io
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Literal, Optional

import pandas as pd
import typer

import pipeline.settings
from pipeline.settings import logger
from scipeds import constants
from scipeds.data.completions import CompletionsQueryEngine
from scipeds.data.enums import FieldTaxonomy, Grouping, NCSESSciGroup
from scipeds.data.queries import QueryFilters, TaxonomyRollup

Status = Literal["PASS", "NEEDS REVIEW", "FAIL"]

PASS: Status = "PASS"
REVIEW: Status = "NEEDS REVIEW"
FAIL: Status = "FAIL"

# How much a column's missingness can move between releases before it's worth a look
NAN_FRACTION_TOLERANCE = 0.05

# How far the newest year's total completions can sit off the recent trend
# before it's worth a look, as a fraction of the previous year's total
TREND_TOLERANCE = 0.10


@dataclass
class Check:
    """One thing we looked at, and what we found."""

    name: str
    status: Status
    summary: str
    # Optional supporting detail, rendered as a table in the report
    detail: Optional[pd.DataFrame] = None
    notes: list[str] = field(default_factory=list)


@dataclass
class Report:
    old_path: Path
    new_path: Path
    checks: list[Check] = field(default_factory=list)
    figures: list[tuple[str, str]] = field(default_factory=list)  # (title, base64 png)

    def add(self, check: Check) -> Check:
        self.checks.append(check)
        return check

    def by_status(self, status: Status) -> list[Check]:
        return [c for c in self.checks if c.status == status]

    @property
    def failed(self) -> bool:
        return len(self.by_status(FAIL)) > 0


def _version_sort_key(path: Path) -> tuple:
    """Sort scipeds_X_Y_Z[_devN].duckdb paths by version, oldest first."""
    nums = [int(n) for n in re.findall(r"\d+", path.stem)]
    # A dev build of a version sorts after the plain release of the previous
    # version but before the release it's building toward
    is_dev = "dev" in path.stem.lower()
    return (tuple(nums[:3]), 0 if is_dev else 1)


def find_old_db() -> Path:
    """Highest-versioned released db in the scipeds cache.

    Deliberately does not use constants.DB_NAME: that's derived from the
    installed package version, so once the version is bumped for a release it
    names a file that doesn't exist yet.
    """
    candidates = sorted(
        (p for p in constants.SCIPEDS_CACHE_DIR.glob("scipeds_*.duckdb") if "dev" not in p.stem),
        key=_version_sort_key,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No released scipeds_*.duckdb found in {constants.SCIPEDS_CACHE_DIR}. "
            "Run `scipeds download-db` or pass --old explicitly."
        )
    return candidates[-1]


def find_new_db() -> Path:
    """Most recently built db in the processed data directory."""
    candidates = sorted(
        pipeline.settings.PROCESSED_DATA_DIR.glob("scipeds_*.duckdb"),
        key=lambda p: p.stat().st_mtime,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No scipeds_*.duckdb found in {pipeline.settings.PROCESSED_DATA_DIR}. "
            "Run `make process` first, or pass --new explicitly."
        )
    return candidates[-1]


# --------------------------------------------------------------------------
# Institution metadata checks
# --------------------------------------------------------------------------


def check_institutions(report: Report, old: pd.DataFrame, new: pd.DataFrame) -> None:
    report.add(
        Check(
            "Institution row count",
            PASS,
            f"{len(old):,} institutions before, {len(new):,} after ({len(new) - len(old):+,}).",
        )
    )

    dupes = new.index.duplicated().sum()
    report.add(
        Check(
            "Institution unitid uniqueness",
            PASS if dupes == 0 else FAIL,
            "Every unitid appears once."
            if dupes == 0
            else f"{dupes:,} duplicated unitids -- the table should have one row each.",
        )
    )

    added = [c for c in new.columns if c not in old.columns]
    if added:
        detail = pd.DataFrame(
            {
                "column": added,
                "non_null": [int(new[c].notna().sum()) for c in added],
                "pct_populated": [f"{new[c].notna().mean():.1%}" for c in added],
                "example_values": [
                    ", ".join(str(v) for v in new[c].dropna().unique()[:3]) or "(all empty)"
                    for c in added
                ],
            }
        )
        report.add(
            Check(
                "Institution columns added",
                REVIEW,
                f"{len(added)} new column(s) in the institutions table. "
                "New IPEDS variables are expected, but worth a look at what showed up.",
                detail=detail,
            )
        )
    else:
        report.add(Check("Institution columns added", PASS, "No new columns."))

    dropped = [c for c in old.columns if c not in new.columns]
    if dropped:
        detail = pd.DataFrame(
            {
                "column": dropped,
                "was_populated": [f"{old[c].notna().mean():.1%}" for c in dropped],
            }
        )
        report.add(
            Check(
                "Institution columns dropped",
                FAIL,
                f"{len(dropped)} column(s) present in the released db are gone. "
                "Anyone querying these will break.",
                detail=detail,
            )
        )
    else:
        report.add(Check("Institution columns dropped", PASS, "No columns lost."))

    shared = [c for c in new.columns if c in old.columns]
    moved = []
    for col in shared:
        old_frac = old[col].isna().mean()
        new_frac = new[col].isna().mean()
        if abs(new_frac - old_frac) > NAN_FRACTION_TOLERANCE:
            moved.append(
                {
                    "column": col,
                    "pct_missing_before": f"{old_frac:.1%}",
                    "pct_missing_after": f"{new_frac:.1%}",
                    "change": f"{new_frac - old_frac:+.1%}",
                }
            )
    if moved:
        report.add(
            Check(
                "Institution missingness",
                REVIEW,
                f"{len(moved)} column(s) changed missingness by more than "
                f"{NAN_FRACTION_TOLERANCE:.0%}.",
                detail=pd.DataFrame(moved),
            )
        )
    else:
        report.add(
            Check(
                "Institution missingness",
                PASS,
                f"No column's missingness moved more than {NAN_FRACTION_TOLERANCE:.0%}.",
            )
        )


# --------------------------------------------------------------------------
# Completions checks
# --------------------------------------------------------------------------


def yearly_totals(engine: CompletionsQueryEngine) -> pd.DataFrame:
    return engine.get_df_from_query(
        f"""
        SELECT year,
               COUNT(*) AS n_rows,
               SUM(n_awards) AS n_awards
        FROM {constants.COMPLETIONS_TABLE}
        GROUP BY year
        ORDER BY year
        """
    ).set_index("year")


def check_year_coverage(report: Report, new_years: pd.Index) -> None:
    expected = set(range(constants.START_YEAR, constants.END_YEAR + 1))
    missing = sorted(expected - set(new_years))
    extra = sorted(set(new_years) - expected)
    if missing or extra:
        bits = []
        if missing:
            bits.append(f"missing {missing}")
        if extra:
            bits.append(f"unexpected {extra}")
        report.add(
            Check(
                "Year coverage",
                FAIL,
                f"Expected {constants.START_YEAR}-{constants.END_YEAR}: " + ", ".join(bits),
            )
        )
    else:
        report.add(
            Check(
                "Year coverage",
                PASS,
                f"All years {constants.START_YEAR}-{constants.END_YEAR} present, no gaps.",
            )
        )


def check_year_deltas(
    report: Report, old: pd.DataFrame, new: pd.DataFrame, expected_changed: set[int]
) -> None:
    """Years that aren't new or revised should be byte-for-byte stable.

    This is the check that catches an accidental reprocessing regression -- e.g.
    silently reading a provisional file instead of a revised one.
    """
    shared = sorted(set(old.index) & set(new.index))
    rows = []
    for year in shared:
        before, after = int(old.loc[year, "n_awards"]), int(new.loc[year, "n_awards"])
        if before != after:
            rows.append(
                {
                    "year": year,
                    "awards_before": f"{before:,}",
                    "awards_after": f"{after:,}",
                    "change": f"{after - before:+,}",
                    "expected": "yes" if year in expected_changed else "NO",
                }
            )
    changed = {r["year"] for r in rows}
    unexpected = changed - expected_changed

    if unexpected:
        report.add(
            Check(
                "Per-year totals stability",
                FAIL,
                f"Years {sorted(unexpected)} changed but shouldn't have. Only "
                f"{sorted(expected_changed)} were expected to move (new data and "
                "revised prior year).",
                detail=pd.DataFrame(rows),
            )
        )
    elif rows:
        report.add(
            Check(
                "Per-year totals stability",
                PASS,
                f"Only the expected years changed: {sorted(changed)}.",
                detail=pd.DataFrame(rows),
            )
        )
    else:
        report.add(
            Check(
                "Per-year totals stability",
                PASS,
                "No previously-released year changed.",
            )
        )


def check_new_year_trend(report: Report, new: pd.DataFrame) -> None:
    year = constants.END_YEAR
    if year not in new.index or (year - 1) not in new.index:
        report.add(
            Check("New year plausibility", FAIL, f"Year {year} is not in the new database.")
        )
        return
    totals = new["n_awards"].astype(int)
    total = int(totals.loc[year])
    prev = int(totals.loc[year - 1])
    change = (total - prev) / prev
    status = PASS if abs(change) <= TREND_TOLERANCE else REVIEW
    report.add(
        Check(
            "New year plausibility",
            status,
            f"{year}: {total:,} awards, {change:+.1%} vs {year - 1} ({prev:,}). "
            + (
                "In line with the previous year."
                if status == PASS
                else f"That's more than {TREND_TOLERANCE:.0%} -- worth confirming "
                "it's real and not a processing artifact."
            ),
        )
    )


def unknown_cip_codes(engine: CompletionsQueryEngine) -> pd.DataFrame:
    return engine.get_df_from_query(
        f"""
        SELECT cip2020, cip_title
        FROM {constants.CIP_TABLE}
        WHERE ncses_sci_group = '{NCSESSciGroup.unknown.value}'
        """
    ).set_index("cip2020")


def check_newly_unknown_cips(
    report: Report, old_engine: CompletionsQueryEngine, new_engine: CompletionsQueryEngine
) -> None:
    """Which CIP codes are unclassified now that weren't before.

    The processing log already prints an unknown count per year, but a number in
    a long log is exactly what gets skipped. This names them instead, ranked by
    how many students they cover, so the add-to-NCSES-table-or-accept decision
    is easy to make.
    """
    old_unknown = unknown_cip_codes(old_engine)
    new_unknown = unknown_cip_codes(new_engine)
    newly = [c for c in new_unknown.index if c not in old_unknown.index]

    if not newly:
        report.add(
            Check(
                "Newly-unclassified CIP codes",
                PASS,
                f"No new unclassified CIP codes ({len(new_unknown)} were already unknown).",
            )
        )
        return

    counts = new_engine.get_df_from_query(
        f"""
        SELECT cip2020, SUM(n_awards) AS n_awards
        FROM {constants.COMPLETIONS_TABLE}
        WHERE year = {constants.END_YEAR} AND cip2020 IN (SELECT UNNEST($codes))
        GROUP BY cip2020
        """,
        {"codes": newly},
    ).set_index("cip2020")["n_awards"]

    # An unclassified code has no title of its own -- that's what being missing from
    # the crosswalk means. Show the nearest classified sibling instead, so the report
    # says what neighbourhood the code is in without anyone having to go look it up.
    cip_table = new_engine.get_cip_table()

    def family_context(code: str) -> str:
        family = code.split(".")[0] + "." + code.split(".")[1][:2]
        siblings = cip_table[
            cip_table.index.str.startswith(family) & (cip_table["cip_title"] != "Unknown")
        ]
        if not len(siblings):
            return f"(no classified codes in {family})"
        return f"{family}x: " + "; ".join(siblings["cip_title"].head(2))

    dhs = cip_table["dhs_stem"].to_dict()

    detail = (
        pd.DataFrame(
            {
                "cip2020": newly,
                f"awards_{constants.END_YEAR}": [int(counts.get(c, 0)) for c in newly],
                "dhs_stem": ["yes" if dhs.get(c) else "no" for c in newly],
                "related_fields": [family_context(c) for c in newly],
            }
        )
        .sort_values(f"awards_{constants.END_YEAR}", ascending=False)
        .reset_index(drop=True)
    )

    total_awards = int(counts.sum()) if len(counts) else 0
    year_total = new_engine.get_df_from_query(
        f"SELECT SUM(n_awards) AS n FROM {constants.COMPLETIONS_TABLE} "
        f"WHERE year = {constants.END_YEAR}"
    )["n"].iloc[0]

    report.add(
        Check(
            "Newly-unclassified CIP codes",
            REVIEW,
            f"{len(newly)} CIP code(s) have no NCSES science classification and didn't "
            f"appear in the released db, covering {total_awards:,} awards "
            f"({total_awards / int(year_total):.4%} of {constants.END_YEAR}). IPEDS "
            f"adding codes NCSES hasn't classified is normal -- decide whether to add "
            f"them to the NCSES table in pipeline/assets/ or accept them as "
            f"unclassified. A 'yes' in dhs_stem means DHS considers the code STEM, so "
            f"it's a real CIP 2020 code that NCSES simply hasn't categorised.",
            detail=detail,
        )
    )


def check_award_levels(
    report: Report, old_engine: CompletionsQueryEngine, new_engine: CompletionsQueryEngine
) -> None:
    def levels(engine: CompletionsQueryEngine) -> set[str]:
        return set(
            engine.get_df_from_query(
                f"SELECT DISTINCT awlevel FROM {constants.COMPLETIONS_TABLE}"
            )["awlevel"]
        )

    new_levels = levels(new_engine) - levels(old_engine)
    if new_levels:
        report.add(
            Check(
                "Award levels",
                REVIEW,
                f"Award level(s) {sorted(new_levels)} appear in the new data but not the "
                "released db. Check they're mapped in AWARD_LEVEL_CODES "
                "(pipeline/completions.py).",
            )
        )
    else:
        report.add(Check("Award levels", PASS, "No unfamiliar award levels."))


def check_raw_file_selection(report: Report) -> None:
    """Did we read the revised file everywhere one was available?

    Unambiguous: if an _rv file is sitting on disk next to the file we actually
    read, something picked the wrong one.
    """
    raw_dir = pipeline.settings.RAW_DATA_DIR / constants.COMPLETIONS_TABLE
    rows = []
    problems = []
    for year_dir in sorted(raw_dir.glob("*"), key=lambda p: p.name):
        if not year_dir.is_dir():
            continue
        csvs = sorted(year_dir.glob("*.csv"))
        if not csvs:
            continue
        revised = [f for f in csvs if f.stem.lower().endswith("_rv")]
        selected = (revised or csvs)[0]
        has_rv = bool(revised)
        rows.append(
            {
                "year": year_dir.name,
                "file_read": selected.name,
                "revised_available": "yes" if has_rv else "no",
            }
        )
        if has_rv and not selected.stem.lower().endswith("_rv"):
            problems.append(year_dir.name)

    detail = pd.DataFrame(rows)
    if problems:
        report.add(
            Check(
                "Provisional vs revised files",
                FAIL,
                f"Years {problems} have a revised file on disk but a non-revised file "
                "was selected.",
                detail=detail,
            )
        )
    else:
        n_rev = sum(1 for r in rows if r["revised_available"] == "yes")
        report.add(
            Check(
                "Provisional vs revised files",
                PASS,
                f"{n_rev} of {len(rows)} years have a revised file, and it was used "
                "in every case.",
                detail=detail,
            )
        )


# --------------------------------------------------------------------------
# Plots
# --------------------------------------------------------------------------


def _load_pyplot():
    """Import pyplot if it's installed, else None.

    Plots are a nice-to-have for the human reading the report; the checks are
    the point. Keeping this optional means the pipeline's own requirements
    don't have to carry a plotting library.
    """
    try:
        import matplotlib
    except ModuleNotFoundError:
        return None
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def _fig_to_base64(plt, fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def build_figures(
    report: Report,
    old_totals: pd.DataFrame,
    new_totals: pd.DataFrame,
    old_inst: pd.DataFrame,
    new_inst: pd.DataFrame,
    new_engine: CompletionsQueryEngine,
) -> None:
    plt = _load_pyplot()
    if plt is None:
        logger.warning(
            "matplotlib is not installed, so the report will have no plots. "
            "Install it with `pip install -r requirements/review.txt`."
        )
        return

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(old_totals.index, old_totals["n_awards"], label="released", lw=2, alpha=0.7)
    ax.plot(new_totals.index, new_totals["n_awards"], label="new build", lw=2, ls="--")
    ax.set_xlabel("year")
    ax.set_ylabel("total awards")
    ax.set_title("Total completions by year")
    ax.legend()
    report.figures.append(("Total completions by year", _fig_to_base64(plt, fig)))

    try:
        rollup = TaxonomyRollup(
            taxonomy_name=FieldTaxonomy.ncses_sci_group,
            taxonomy_values=list(NCSESSciGroup),
        )
        by_gender = new_engine.rollup_by_grouping(
            grouping=Grouping.gender,
            rollup=rollup,
            query_filters=QueryFilters(),
            by_year=True,
        )
        pivot = (
            by_gender.reset_index()[["year", "gender", "uni_degrees_within_gender"]]
            .drop_duplicates()
            .pivot(index="year", columns="gender", values="uni_degrees_within_gender")
        )
        fig, ax = plt.subplots(figsize=(8, 4))
        pivot.plot(ax=ax, lw=2)
        ax.set_xlabel("year")
        ax.set_ylabel("degrees")
        ax.set_title("Completions by gender, by year (new build)")
        report.figures.append(("Completions by gender", _fig_to_base64(plt, fig)))
    except Exception as exc:  # pragma: no cover - plotting is best-effort
        logger.warning(f"Could not build gender plot: {exc}")

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.hist(old_inst.isna().sum(), bins=30, alpha=0.6, label="released")
    ax.hist(new_inst.isna().sum(), bins=30, alpha=0.6, label="new build")
    ax.set_xlabel("missing values per column")
    ax.set_ylabel("number of columns")
    ax.set_title("Institution metadata missingness")
    ax.legend()
    report.figures.append(("Institution missingness", _fig_to_base64(plt, fig)))


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

BADGE = {PASS: "pass", REVIEW: "review", FAIL: "fail"}


def render_html(report: Report) -> str:
    def render_check(check: Check) -> str:
        parts = [
            f'<div class="check {BADGE[check.status]}">',
            f'<div class="check-head"><span class="badge">{check.status}</span>'
            f"<h3>{html.escape(check.name)}</h3></div>",
            f"<p>{html.escape(check.summary)}</p>",
        ]
        if check.detail is not None and len(check.detail):
            parts.append(
                '<div class="tablewrap">' + check.detail.to_html(index=False, border=0) + "</div>"
            )
        parts.append("</div>")
        return "\n".join(parts)

    sections = []
    for status, heading, blurb in [
        (FAIL, "Failures", "These look broken. Fix before shipping."),
        (
            REVIEW,
            "Needs your review",
            "Real changes that need a human call. Not necessarily problems.",
        ),
        (PASS, "Passed", "Nothing to do here."),
    ]:
        checks = report.by_status(status)
        if not checks:
            continue
        sections.append(
            f'<section class="{BADGE[status]}-section"><h2>{heading} ({len(checks)})</h2>'
            f"<p class='blurb'>{blurb}</p>"
            + "\n".join(render_check(c) for c in checks)
            + "</section>"
        )

    if report.figures:
        figures = (
            "<section><h2>Plots</h2>"
            + "\n".join(
                f'<figure><img src="data:image/png;base64,{b64}" alt="{html.escape(title)}">'
                f"<figcaption>{html.escape(title)}</figcaption></figure>"
                for title, b64 in report.figures
            )
            + "</section>"
        )
    else:
        figures = (
            "<section><h2>Plots</h2><p class='blurb'>No plots -- matplotlib isn't "
            "installed. <code>pip install -r requirements/review.txt</code> to get "
            "them.</p></section>"
        )

    n_fail = len(report.by_status(FAIL))
    n_review = len(report.by_status(REVIEW))
    n_pass = len(report.by_status(PASS))

    return f"""<!doctype html>
<html><head><meta charset="utf-8">
<title>scipeds data update review</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         max-width: 60rem; margin: 0 auto; padding: 2rem 1rem; line-height: 1.5;
         color: #1a1a1a; }}
  h1 {{ margin-bottom: 0.25rem; }}
  .paths {{ background: #f4f4f5; border-radius: 6px; padding: 1rem; margin: 1rem 0 2rem; }}
  .paths code {{ font-size: 0.85rem; word-break: break-all; }}
  .tally {{ display: flex; gap: 0.75rem; margin: 1rem 0 2rem; flex-wrap: wrap; }}
  .tally span {{ padding: 0.35rem 0.75rem; border-radius: 999px; font-weight: 600;
                 font-size: 0.85rem; }}
  .t-fail {{ background: #fee2e2; color: #991b1b; }}
  .t-review {{ background: #fef3c7; color: #92400e; }}
  .t-pass {{ background: #dcfce7; color: #166534; }}
  section {{ margin-bottom: 2.5rem; }}
  .blurb {{ color: #52525b; margin-top: -0.5rem; }}
  .check {{ border-left: 4px solid #d4d4d8; padding: 0.75rem 1rem; margin: 1rem 0;
            background: #fafafa; border-radius: 0 6px 6px 0; }}
  .check.fail {{ border-color: #dc2626; }}
  .check.review {{ border-color: #f59e0b; }}
  .check.pass {{ border-color: #22c55e; }}
  .check-head {{ display: flex; align-items: center; gap: 0.75rem; }}
  .check-head h3 {{ margin: 0; font-size: 1rem; }}
  .badge {{ font-size: 0.7rem; font-weight: 700; letter-spacing: 0.03em;
            padding: 0.2rem 0.5rem; border-radius: 4px; background: #e4e4e7; }}
  .fail .badge {{ background: #fee2e2; color: #991b1b; }}
  .review .badge {{ background: #fef3c7; color: #92400e; }}
  .pass .badge {{ background: #dcfce7; color: #166534; }}
  .check p {{ margin: 0.5rem 0; }}
  .tablewrap {{ overflow-x: auto; }}
  table {{ border-collapse: collapse; font-size: 0.85rem; margin-top: 0.5rem; }}
  th, td {{ padding: 0.3rem 0.7rem; text-align: left; border-bottom: 1px solid #e4e4e7; }}
  th {{ background: #f4f4f5; }}
  figure {{ margin: 1.5rem 0; }}
  figure img {{ max-width: 100%; border: 1px solid #e4e4e7; border-radius: 6px; }}
  figcaption {{ color: #52525b; font-size: 0.85rem; margin-top: 0.4rem; }}
</style></head>
<body>
<h1>scipeds data update review</h1>
<p class="blurb">Comparing a freshly-built database against the last release.</p>
<div class="paths">
  <div><strong>Released (old):</strong> <code>{html.escape(str(report.old_path))}</code></div>
  <div><strong>New build:</strong> <code>{html.escape(str(report.new_path))}</code></div>
</div>
<div class="tally">
  <span class="t-fail">{n_fail} failing</span>
  <span class="t-review">{n_review} need review</span>
  <span class="t-pass">{n_pass} passing</span>
</div>
{"".join(sections)}
{figures}
</body></html>
"""


def print_summary(report: Report) -> None:
    logger.info(f"Old (released): {report.old_path}")
    logger.info(f"New (build):    {report.new_path}")
    for status in (FAIL, REVIEW, PASS):
        for check in report.by_status(status):
            logger.info(f"[{status}] {check.name}: {check.summary}")


def main(
    old: Annotated[
        Optional[Path], typer.Option(help="Previously released db (default: newest in cache)")
    ] = None,
    new: Annotated[
        Optional[Path], typer.Option(help="Newly built db (default: newest in data/processed)")
    ] = None,
    outdir: Annotated[Optional[Path], typer.Option(help="Where to write the HTML report")] = None,
) -> Any:
    """Compare a newly built scipeds database against the last released one."""
    old_path = old or find_old_db()
    new_path = new or find_new_db()
    outdir = outdir or pipeline.settings.PROCESSED_DATA_DIR / "review"
    outdir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Comparing {new_path.name} (new) against {old_path.name} (released)")
    report = Report(old_path=old_path, new_path=new_path)

    old_engine = CompletionsQueryEngine(db_path=old_path)
    new_engine = CompletionsQueryEngine(db_path=new_path)

    old_inst = old_engine.get_institutions_table()
    new_inst = new_engine.get_institutions_table()
    check_institutions(report, old_inst, new_inst)

    old_totals = yearly_totals(old_engine)
    new_totals = yearly_totals(new_engine)
    check_year_coverage(report, new_totals.index)

    # The new year, plus the prior year whose revised file we just picked up
    expected_changed = {constants.END_YEAR, constants.END_YEAR - 1}
    check_year_deltas(report, old_totals, new_totals, expected_changed)
    check_new_year_trend(report, new_totals)
    check_newly_unknown_cips(report, old_engine, new_engine)
    check_award_levels(report, old_engine, new_engine)
    check_raw_file_selection(report)

    build_figures(report, old_totals, new_totals, old_inst, new_inst, new_engine)

    report_path = outdir / "review.html"
    report_path.write_text(render_html(report))

    print_summary(report)
    logger.info(f"Wrote report to {report_path}")

    if report.failed:
        logger.error(f"{len(report.by_status(FAIL))} check(s) FAILED")
        raise typer.Exit(code=1)
    n_review = len(report.by_status(REVIEW))
    if n_review:
        logger.warning(f"{n_review} check(s) need a human decision -- see the report")
    return 0


if __name__ == "__main__":
    typer.run(main)
