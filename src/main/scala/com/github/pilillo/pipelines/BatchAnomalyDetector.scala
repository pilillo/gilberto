package com.github.pilillo.pipelines

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers.{Analyzer, DoubleValuedState}
import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.amazon.deequ.checks.CheckStatus.Success
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.mastro.MastroMetricsRepository
import com.amazon.deequ.repository.querable.QuerableMetricsRepository
import com.github.pilillo.commons.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object BatchAnomalyDetector {
  val log : Logger = Logger.getLogger(getClass.getName)

  def getAnomalyCheckAnalyzer[S <: DoubleValuedState[_]](codeConfig : String) : (AnomalyDetectionStrategy, Analyzer[S, DoubleMetric]) = {
    val toolbox = currentMirror.mkToolBox()
    //val toolbox = reflect.runtime.universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val ast = toolbox.parse(codeConfig)

    toolbox.eval(ast).asInstanceOf[(AnomalyDetectionStrategy, Analyzer[S, DoubleMetric])]
  }

  implicit class Detector(df: DataFrame) {
    def detect(codeConfig : String, repository : String): Int = {
      if(repository == null || repository.isEmpty) {
        //verifier
        log.error("No repository specified")
        return 4
      }

      // todo: use current date in tags?
      val tags = Map[String,String]()
      val resultKey = ResultKey(System.currentTimeMillis(), tags)

      val verifier = VerificationSuite().onData(df)

      // if a valid url is provided, use the mastro repo - otherwise save to file system
      val repo = if(Utils.urlValidator.isValid(repository)){
          MastroMetricsRepository(df.sparkSession, endpoint = repository)
      }else{
        //FileSystemMetricsRepository(df.sparkSession, metricsRepo)
        QuerableMetricsRepository(df.sparkSession, path = repository)
      }

      val (detectionStrategy, analyzer) = getAnomalyCheckAnalyzer(codeConfig)

      val verificationResult = verifier
        .useRepository(repo)
        .saveOrAppendResult(resultKey)
        .addAnomalyCheck(detectionStrategy, analyzer)
        .run()

      if (verificationResult.status == Success) {
        log.info("The data passed the anomaly check, everything is fine!")
        0
      }else {
        log.error("Anomaly detected in following metrics!")
        // logging metrics
        repo
          .load()
          // this returns all metrics as df
          .getSuccessMetricsAsDataFrame(df.sparkSession)
          .show()
        /*
        https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/anomaly_detection_example.md
        Something like this:
        +-------+--------+----+-----+-------------+
        | entity|instance|Name|value| dataset_date|
        +-------+--------+----+-----+-------------+
        |Dataset|       *|Size|  2.0|1538384009558|
        |Dataset|       *|Size|  5.0|1538385453983|
        +-------+--------+----+-----+-------------+
         */
        // failing pipeline - return error
        4
      }

    }
  }
}
