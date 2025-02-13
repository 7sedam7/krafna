# Changelog

## [0.x.x] - 2025-02-13
### Added
- Added tests for executor (select, where, order_by)
- Added as_(list, string, f64, bool) to FieldValue
- Added DATE function that parses date from string
- DATEADD function can add optional 4th argument for format

### Changed
- No changes

### Fixed
- No known bugs atm

## [0.3.0] - 2025-02-01
### Added
- Accessing nested fields with '.' in field names
- Added default values (today, now)
- Implemented DATEADD function

### Changed
- add select (as override of select)

### Fixed
- No known bugs atm
