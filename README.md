# dbt-sail

The [Sail](https://github.com/lakehq/sail) adapter plugin for [dbt](https://www.getdbt.com/).

[Sail](https://github.com/lakehq/sail) is a drop-in replacement for Apache Spark, built and maintained by [LakeSail](https://lakesail.com/). Sail implements the Spark Connect protocol, so `dbt-sail` is a thin wrapper around `dbt-spark` that connects to Sail instead of JVM Spark. `dbt-sail` supports the same feature set as `dbt-spark`.

## Status

`dbt-sail` is under active development. LakeSail maintains and supports the adapter, which evolves alongside Sail as new capabilities are added.

## Installation

`dbt-sail` does not install PySpark, so you can choose the PySpark version. Install it alongside a Spark Connect client:

```bash
pip install dbt-sail "pyspark-client==4.2.0"
```

For Spark 3.5, use `pyspark[connect]` instead:

```bash
pip install dbt-sail "pyspark[connect]==3.5.9"
```

`pyspark-client` is the lightweight client introduced in Spark 4.0. It leaves out the Spark JARs, which Sail does not need.

## Community

- Report `dbt-sail` adapter issues on the [dbt-sail issue tracker](https://github.com/lakehq/dbt-sail/issues).
- Report Sail engine issues on the [Sail issue tracker](https://github.com/lakehq/sail/issues).
- Join the [LakeSail Slack community](https://www.launchpass.com/lakesail-community/free) for questions and discussion.

## License

Licensed under Apache License 2.0. See [LICENSE](LICENSE).
