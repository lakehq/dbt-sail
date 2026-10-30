# dbt-sail Changelog

- This file provides a full account of all changes to `dbt-sail`.
- Changes are listed under the (pre)release in which they first appear. Subsequent releases include changes from previous releases.
- "Breaking changes" listed under a version may require action from end users or external maintainers when upgrading to that version.

## dbt-sail 0.2.0 - October 30, 2026

### Breaking Changes

- `dbt-sail` no longer installs PySpark through dbt-spark's `session` extra. Install a Spark Connect client alongside it, such as `pyspark-client` or `pyspark[connect]`.
- In `remote` mode, port 443 with neither `token` nor `use_ssl` is now a configuration error. `dbt-sail` used to switch it to port 50051 without saying so. For a Sail server without TLS, set `port`.

### Features

- `remote` mode passes the profile's `token` and `use_ssl` to the Spark Connect URL, so it can reach a Sail server behind TLS that requires a token.

## dbt-sail 0.1.0 - April 25, 2026

### Features

- Initial release of the Sail adapter plugin for dbt, extracted from the `dbt-adapters` monorepo.
- `dbt init` support for `embedded` and `remote` Spark Connect modes.
