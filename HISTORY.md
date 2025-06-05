# Changelog

## [Unreleased]

## [v0.0.7] (2025-06-05)

### Changed

- Fixed issue with institution directory metadata processing (Issue [#59](https://github.com/scienceforamerica/scipeds/issues/59))
- Update links to point to new scipeds.org URL (PR [#56](https://github.com/scienceforamerica/scipeds/pull/56))
- Update race/ethnicity change warning with link to IPEDS documentation (PR [#57](https://github.com/scienceforamerica/scipeds/pull/57))
- Fix FutureWarning (PR [#58](https://github.com/scienceforamerica/scipeds/pull/58))
- Fixed typos in documentation and docstrings (PR [#60](https://github.com/scienceforamerica/scipeds/pull/60))

## [v0.0.6] (2025-03-20)

### Added

- Added logo, designed by Adrianna Mena (Issue [#51](https://github.com/scienceforamerica/scipeds/issues/51))

### Changed

- Fixed issue with incorrect CIP code classification (Issue [#42](https://github.com/scienceforamerica/scipeds/issues/42))
- Completions queries now return some zero-value aggregates (PR [#49](https://github.com/scienceforamerica/scipeds/pulls/49))
- Taxonomy values parameter for field totals aggregation fixed (Issue [#47](https://github.com/scienceforamerica/scipeds/issues/47))
- Taxonomy rollup handles list-like objects that have `tolist` methods (Issue [#52](https://github.com/scienceforamerica/scipeds/issues/52))

## [v0.0.5] (2025-03-12)

### Added

- 1984-1994 data added (gender only) (Issue [#15](https://github.com/scienceforamerica/scipeds/issues/15))

### Changed
- Force anonymous download client use (PR [#41](https://github.com/scienceforamerica/scipeds/pull/41))
- Math enum name for NCSES Detailed Field Group changed from `math` to `math_stats` (PR [#40](https://github.com/scienceforamerica/scipeds/pull/40))

## [v0.0.4] (2025-02-21)

### Added 

- UNITID filter on uni-level completions queries (Issue [#15](https://github.com/scienceforamerica/scipeds/issues/15))
- Test coverage make command and tests for calculations (Issue [#26](https://github.com/scienceforamerica/scipeds/issues/26))
- GitHub action for releasing (PR [#29](https://github.com/scienceforamerica/scipeds/pull/29))
- Data versioning for processed duckdb file (Issue [#25](https://github.com/scienceforamerica/scipeds/issues/25))
- RELEASE.md to keep track of release instructions (PR [#35](https://github.com/scienceforamerica/scipeds/pull/35))

### Changed

- Updates to the README (PR [#9](https://github.com/scienceforamerica/scipeds/pull/9), PR [#27](https://github.com/scienceforamerica/scipeds/pull/27), PR [#30](https://github.com/scienceforamerica/scipeds/pull/30))
- Fixed geographic region processing (Issue [#16](https://github.com/scienceforamerica/scipeds/issues/16))
- Query improvements (Issues [#14](https://github.com/scienceforamerica/scipeds/issues/14), [#17](https://github.com/scienceforamerica/scipeds/issues/17), [#18](https://github.com/scienceforamerica/scipeds/issues/18), [#19](https://github.com/scienceforamerica/scipeds/issues/19), [#20](https://github.com/scienceforamerica/scipeds/issues/20))
- Docs build fails loudly if notebook errors (PR [#31](https://github.com/scienceforamerica/scipeds/pull/31)) and updated notebook (Issue [#10](https://github.com/scienceforamerica/scipeds/issues/10))

## [v0.0.3] (2024-12-20)

### Added

- Project urls added to pyproject.toml (Issue [#7](https://github.com/scienceforamerica/scipeds/issues/7))

## [v0.0.2] (2024-12-18)

### Changed

- Update README links (PR [#2](https://github.com/scienceforamerica/scipeds/pull/2))
- Clarify overwrite syntax for db download (Issue [#3](https://github.com/scienceforamerica/scipeds/issues/3))
- Fix linting issue due to numpy 2.2.0 (PR [#5](https://github.com/scienceforamerica/scipeds/pull/5))

## [v0.0.1] (2024-11-29)

Initial release! 🎉

[Unreleased]: https://github.com/scienceforamerica/scipeds/compare/v0.0.7...HEAD
[v0.0.7]: https://github.com/scienceforamerica/scipeds/compare/v0.0.6...v0.0.7
[v0.0.6]: https://github.com/scienceforamerica/scipeds/compare/v0.0.5...v0.0.6
[v0.0.5]: https://github.com/scienceforamerica/scipeds/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/scienceforamerica/scipeds/compare/v0.0.3...v0.0.4
[v0.0.3]: https://github.com/scienceforamerica/scipeds/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/scienceforamerica/scipeds/compare/v0.0.1...v0.0.2
[v0.0.1]: https://github.com/scienceforamerica/scipeds/releases/tag/v0.0.1
