package com.amazon.deequ.repository.querable

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultSerde, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import com.github.pilillo.Settings.Configs
import org.apache.spark.sql.{Encoders, SparkSession}

class QuerableMetricsRepository (session: SparkSession, path: String, coalesce : Int = Configs.NUM_TARGET_PARTITIONS)
  extends MetricsRepository{

  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)
    val result = AnalysisResult(resultKey, analyzerContextWithSuccessfulValues)

    QuerableMetricsRepository.write(session, path, result)
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = {
    load()
      // todo: add search - e.g. by tags
      .get().find(_.resultKey == resultKey).map(_.analyzerContext)
  }

  override def load(): MetricsRepositoryMultipleResultsLoader = new QuerableMetricsMultipleResultsLoader(session, path)
}

// companion object
object QuerableMetricsRepository {

  def apply(session: SparkSession, path : String): QuerableMetricsRepository = {
    new QuerableMetricsRepository(session, path)
  }

  def write(session: SparkSession,
            path: String,
            analysisResult : AnalysisResult) : Unit = {
    val resultDataFrame = AnalysisResult.getSuccessMetricsAsDataFrame(session, analysisResult)
    resultDataFrame
      .write
      .mode("overwrite")
      .parquet(path)
  }

  def read(session: SparkSession,
           path : String,
           tagValues: Option[Map[String, String]]
  ): Option[String] = {
    /*
    Some(
      session
        .read
        .parquet(path)
        .map(row => row.mkString(""), Encoders.STRING())
        .collect()
        .mkString("\n")
    )

     */
    // todo: enjoy
    None
  }
}