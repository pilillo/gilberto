package com.amazon.deequ.repository.querable

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultSerde, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import com.github.pilillo.Settings.Configs
import com.github.pilillo.commons.Utils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

class QuerableMetricsRepository (session: SparkSession, path: String, coalesce : Int)
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

  def apply(session: SparkSession, path : String, coalesce : Int = Configs.NUM_TARGET_PARTITIONS): QuerableMetricsRepository = {
    new QuerableMetricsRepository(session, path, coalesce)
  }

  def write(session: SparkSession,
            path: String,
            analysisResult : AnalysisResult) : Unit = {

    // get as DF, this does already convert all tags to columns that we can then partition-by on
    val resultDataFrame = AnalysisResult.getSuccessMetricsAsDataFrame(session, analysisResult)

    val writer = resultDataFrame
      .write
      .mode("overwrite")

    val partitionedWriter = if(analysisResult.resultKey.tags.keys.size > 0)
      writer.partitionBy(analysisResult.resultKey.tags.keys.toList : _*)
    else writer

    partitionedWriter.parquet(path)
  }

  def read(session: SparkSession,
           path : String,
           tagValues: Option[Map[String, String]]
  ): Option[DataFrame] = {
    val df = if(FileSystem.get(session.sparkContext.hadoopConfiguration).exists(new Path(path)))
      session.read.parquet(path)
    else session.emptyDataFrame

    Some(
      tagValues.getOrElse(Map.empty).foldLeft(df){
        case (agg, (k, v)) => agg.where(s"${k} = ${v}")
      }
    )
  }
}