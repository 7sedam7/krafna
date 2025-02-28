# Changelog:

## [0.5.0] - 2025-02-28
### Added
- Added parsing of markdown file
  - code parsed from ast instead of regex
  - links parsed
  - tasks parsed
- Added corresponding functions that can be used in FROM (check README for usage)
  - MD_LINKS
  - MD_TASKS

### Changed
- No changes

### Fixed
- Tests checking conversion to json string when serialising Hash were not reliabse since element order is not guaranteed


## [0.4.1] - 2025-02-13
### Added
- Added parsing of NULL

### Changed
- No dry run for expression evaluation

### Fixed
- If one document fails expression evaluation, it should not stop the whole evaluation


## [0.4.0] - 2025-02-13
### Added
- Added tests for executor
- Added as_(list, string, f64, bool) to FieldValue
- Added implementation for rest of the operators
- Added DATE function that parses date from string
- DATEADD function can add optional 4th argument for format

### Changed
- No changes

### Fixed
- Fixed issue with adding operator to stack (all, not just last operator on the stack that's higher or equal precedence gets popped and evaluated)
- Improved regex handling

## [0.3.0] - 2025-02-01
### Added
- Accessing nested fields with '.' in field names
- Added default values (today, now)
- Implemented DATEADD function

### Changed
- add select (as override of select)

### Fixed
- No known bugs atm
