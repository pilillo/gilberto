# Gilberto

A nice guy you may rely on for your Data Quality;

## Pipelines

Gilberto uses [AWS Deequ](https://github.com/awslabs/deequ) and [Apache Spark](https://spark.apache.org/) at its core.

Specifically, you can instantiate:
* `profile` to perform profiling of any Spark DataFrame - loaded from a Hive table
* `suggest` to perform constraint suggestion based on selected data distribution
* `validate` to perform a data quality validation step, based on an input ***check*** file
* `detect` to perform anomaly detection, based on an imput ***strategy*** file

Gilberto is meant to be run as a step within a workflow manager (e.g. with the workflow failing in case of data inconsistencies), pulling data from a remote Hadoop/Hive cluster or S3/Presto datalake and pushing data to specific MetricsRepositories.

## Usage

The input arguments are handled using [scopt](https://github.com/scopt/scopt). Gilberto is expected a data source, an action (or pipeline) to perform on the data, as well as a time interval `(from, to)`, along with a destination file path.
For the validator, Gilberto also expects a repository target, an endpoint or a file path, along with a code config file specifying the checks to perform on source data.
For the profiler and the suggester, you may also specify a list of columns to use for partitioning the resulting dataframe,
such as `PROC_YEAR,PROC_MONTH,PROC_DAY` to use processing date columns or respectively `START_*` and `END_*` for the beginning and end of the selected date interval.

Specifically:
```bash
  -a, --action action             Action is the pipeline to be run
  -s, --source source             Source defines where to load data from
  -d, --destination destination   Destination defines where to save results
  -f, --from date                 Beginning of the time interval
  -t, --to date                   End of the time interval
  -r, --repository target         Target folder or endpoint of the repository
  -c, --code-config-path path     Path of the file containing the checks to instruct the validator
  -p, --partition-by columns      Columns to use to partition the resulting dataframe
```

Here an example check file `checks.gibo` for the `validate` step:
```scala
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
Seq(
  Check(CheckLevel.Error, "data testing with error level")
    .hasSize(_ >0)
    .hasMin("numViews", _ > 12)
)
```

The file is interpreted as Scala code using reflection and applied as Checks (see [`addChecks`](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/VerificationRunBuilder.scala#L86)) on a Validator instance.

Similarly, here is an example strategy file `strategy.gibo` for the `detect` step:
```scala
import com.amazon.deequ.anomalydetection._
import com.amazon.deequ.analyzers._
(
  RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),
  Size()
)
```
which is interpreted as tuple of kind `AnomalyDetectionStrategy, Analyzer[S, DoubleMetric]` and applied to a Detector instance (see [anomaly detection example](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/anomaly_detection_example.md)).


## Metrics repositories

Deequ provides an [InMemoryMetricsRepository](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/repository/memory/InMemoryMetricsRepository.scala) and a [FileSystemMetricsRepository](https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/repository/fs/FileSystemMetricsRepository.scala).
The former is basically a Concurrent Hashmap, while the second is a connector allowing for writing a json file of kind `metrics.json` to HDFS or S3. 
Clearly, this has various drawbacks. Most of all, writing to a unique file blob all metrics does not scale and does not allow for querying from Presto and other engines alike.

We provide the following metrics repositories:
* [`MastroMetricsRepository`](https://github.com/pilillo/gilberto/tree/master/src/main/scala/com/amazon/deequ/repository/mastro) pushing to the [Mastro](https://github.com/data-mill-cloud/mastro) catalogue and metrics repo; consequently allowing for lineage tracking and source discovery;
* [`QuerableMetricsRepository`](https://github.com/pilillo/gilberto/tree/master/src/main/scala/com/amazon/deequ/repository/querable) based on the existing `FileSystemMetricsRepository` but writing DataFrames as partitioned parquet files instead of as a unique json file;

