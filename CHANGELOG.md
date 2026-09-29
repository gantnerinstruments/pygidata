## [0.5.5]

### Added

- `GIDataClient.real_last_ts(source_id)` returns the exact last timestamp of a GI.cloud stream (`GIStream.last_ts` is
  only an estimate). Temporary workaround until the backend provides it.

### Changed

- adapted to new example installation dir on gibench

## [0.5.4]

### Changed

- Improved and split docs for analytics examples and pygidata

## [0.5.3]

### Changed

- Enable all resolutions options on fetch_history / fetch_buffer for GI.cloud exports

## [0.5.2]

### Fixed

- Fixed docs link that was broken in pypi

### Added

- Fully added measurement API funcs

## [0.5.1]

### Added

- Support for python 3.13 and 3.14

### Changed

- Adapted for new pypi publishing mechanism

## [0.5.0]

### Added

- The project "ginsapy" is now part of pygidata as the "ginsutility" module, providing local Highspeedport access via `giutility.dll`.

### Changed

- Docs updated

## [0.4.1]

### Added

- Added new export resolution "RAW" for GI.cloud exports
- Enabled UDBF export
- Added 15 second download progress indication

## [0.4.0]


### Breaking Changes
- Complete redesign of the project support data processing for GI.cloud, GI.bench, Q.core and Q.controller
- Supports now: Async, Type Hints, Pydantic v2, Exposed parameterization, Improved logging, Intellisense support

### Changed

- Docs updated
### Fixed
