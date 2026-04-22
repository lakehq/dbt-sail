# dbt-sail

The [Sail](https://github.com/lakehq/sail) adapter plugin for [dbt](https://www.getdbt.com/).

[Sail](https://github.com/lakehq/sail) is a drop-in replacement for Apache Spark, built and maintained by [LakeSail](https://lakesail.com/). Sail implements the Spark Connect protocol, so `dbt-sail` is a thin wrapper around `dbt-spark` that connects to a Sail instead of Spark. `dbt-sail` supports the same feature set as `dbt-spark`.

## Status

`dbt-sail` is under active development. LakeSail maintains and supports the adapter, which evolves alongside Sail as new capabilities are added.

## Installation

> TODO: publish to PyPI and document `pip install dbt-sail`.

## Documentation

> TODO: link the setup and configuration pages on [docs.getdbt.com](https://docs.getdbt.com) once they're live.

## Community

- Report `dbt-sail` adapter issues on the [dbt-sail issue tracker](https://github.com/lakehq/dbt-sail/issues).
- Report Sail engine issues on the [Sail issue tracker](https://github.com/lakehq/sail/issues).
- Join the [LakeSail Slack community](lakesail-community.slack.com) for questions and discussion.

## License

Licensed under Apache 2.0. See [LICENSE](LICENSE).
