# Deploying to a Yarn Cluster

## Profiling
```bash
USER=<my-user>
PRINCIPAL=${USER}@<my-realm>
DESTINATION=/user/${USER}/gibo_tests/profiling
```

```bash
spark-submit --master yarn --deploy-mode cluster \
--principal ${PRINCIPAL} --keytab ${USER}.keytab \
--properties-file my-spark.conf \
--class "com.github.pilillo.Gilberto" \
--name "Gilberto-tests" \
gilberto-assembly-0.1.jar \
--action profile \
--source <my-input-hive-table> \
--destination ${DESTINATION} \
--from 2021-05-01 --to 2021-05-01
```

You can also partition the result by time, by appending to the submit command:
```bash
--partition-by PROC_YEAR,PROC_MONTH,PROC_DAY
```

which uses the processing time.

## Validate

Firstly, upload a `checks.gibo` file on hdfs containing the following:

```scala
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
Seq(
  Check(CheckLevel.Error, "data testing with error level")
    .hasSize(_ >0)
)
```

Then submit a validation pipeline as follows:
```bash
spark-submit --master yarn --deploy-mode cluster \
--principal ${PRINCIPAL} --keytab ${USER}.keytab \
--properties-file my-spark.conf \
--class "com.github.pilillo.Gilberto" \
--name "Gilberto-tests" \
gilberto-assembly-0.1.jar \
--action validate \
--source <my-input-hive-table> \
--destination ${DESTINATION} \
--from 2021-05-01 --to 2021-05-01 \
--code-config-path hdfs://<my-namenode>/user/${USER}/checks.gibo
```