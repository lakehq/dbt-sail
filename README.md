# dbt-sail

The [Sail](https://github.com/lakehq/sail) adapter plugin for [dbt](https://www.getdbt.com/).

[Sail](https://lakesail.com/) is a drop-in replacement for Apache Spark, built and maintained by [LakeSail](https://lakesail.com/). Since Sail speaks the Spark Connect protocol, `dbt-sail` is a thin wrapper around `dbt-spark` that points at Sail instead of a Spark cluster. Anything `dbt-spark` can do should work against Sail.

## Status

`dbt-sail` is in active development. LakeSail maintains and supports it, but the adapter changes often as Sail adds features. Pin a version if you're running it in production.

## Installation

> TODO: publish to PyPI and document `pip install dbt-sail`.

## Documentation

> TODO: link the setup and configuration pages on [docs.getdbt.com](https://docs.getdbt.com) once they're live.

## Community

File bugs and feature requests on the [GitHub issue tracker](https://github.com/lakehq/sail/issues). You can also join the [LakeSail Slack community](https://www.launchpass.com/lakesail-community/free).

## License

Licensed under Apache 2.0. See [LICENSE](LICENSE).
