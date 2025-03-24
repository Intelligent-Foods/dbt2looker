# Changelog

Recent and upcoming changes to dbt2looker

## 0.11.12 (Not released to pypy)

### Added
- `group_item_label` optional dimension parameter

## 0.11.11 (Not released to pypy)

### Fixed
- do not add 'sql' key to measure configuration if type is 'count', see https://cloud.google.com/looker/docs/reference/param-measure-types#count

## 0.11.10 (Not released to pypy)

### Fixed
- now supports `pydantic` v2

## 0.11.9 (Not released to pypy)

### Added

- `required_access_grants` option to dimensions

## 0.11.8 (Not released to pypy)

### Added

- day_of_week as timeframe option
- hidden option to dimensions (necessary to be able to join on fields we do not want to include)
- suggestions option to dimensions
- drill_fields options to measures

### Changed

- do not check dimensions referenced in measure filters as the check was too strict

## 0.11.7 (Not released to pypy)

### Added

- view level drill fields including all dimensions and dimension groups

## 0.11.6 (Not released to pypy)

### Fixed

- discard ephemaral models instead of parsing failure exception
- re-generate Poetry lockfile so that it matches pyproject.toml (by using `poetry lock --no-update`)

## 0.11.5 (Not released to pypy)

### Added

- support for customizing timeframes and convert_tz for dimension groups

## 0.11.4 (Not released to pypy)

### Added

- support for dimensions defined on the model level

## 0.11.3 (Not released to pypy)

### Added

- support for view_label for dimension groups

## 0.11.2 (Not released to pypy)

### Changed

- indent subsequent lines of multi-line dimension, measure and explore descriptions
  for improved code formatting (no effect on what we see in Looker)

## 0.11.1 (Not released to pypy)

### Added

- support for explore meta fields such as label, view_name, view_label
- support for foreign_key and view_label for joins
- support for label, group_label and view_label for all dimensions and measures

### Changed

- Add model meta config under the config key instead of as a high level key
- Extend timeframe values 

## 0.11.0
### Added
- support label and hidden fields (#49)
- support non-aggregate measures (#41)
- support bytes and bignumeric for bigquery (#75)
- support for custom connection name on the cli (#78)

### Changed
- updated dependencies (#74)

### Fixed
- Types maps for redshift (#76)

### Removed
- Strict manifest validation (#77)

## 0.10.0
### Added
Support for dbt 1.x

### Removed
Support for dbt <1.x

## 0.9.3
### Fixed
- Fix name of models and view to add lkml suffix

## 0.9.2
### Fixed
- Bug in spark adapter

## 0.9.1
### Fixed
- Fixed bug where dbt2looker would crash if a dbt project contained an empty model

### Changed
- When filtering models by tag, models that have no tag property will be ignored

## 0.9.0
### Added
- Support for spark adapter (@chaimt)

### Changed
- Updated with support for dbt2looker (@chaimt)
- Lookml views now populate their "sql_table_name" using the dbt relation name

## 0.8.2
### Changed
- Measures with missing descriptions fall back to coloumn descriptions. If there is no column description it falls back to "{measure_type} of {column_name}".

## 0.8.1
### Added
- Dimensions have an `enabled` flag that can be used to switch off generated dimensions for certain columns with `enabled: false`
- Measures have been aliased with the following: `measures,measure,metrics,metric`

### Changed
- Updated dependencies

## 0.8.0
### Changed
- Command line interface changed argument from `--target` to `--target-dir`

### Added
- Added the `--project-dir` flag to the command line interface to change the search directory for `dbt_project.yml`
