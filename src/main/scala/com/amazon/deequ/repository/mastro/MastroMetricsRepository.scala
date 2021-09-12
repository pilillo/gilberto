package com.amazon.deequ.repository.mastro

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.apache.spark.sql.SparkSession
import scalaj.http.{Http, HttpOptions}

import scala.collection.Map

class MastroMetricsRepository (session: SparkSession, endpoint: String) extends MetricsRepository{
  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)
    val result = AnalysisResult(resultKey, analyzerContextWithSuccessfulValues)

    // put metric to mastro
    MastroMetricsRepository.putToMastro(session, endpoint, result)
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = {
    // get a loader builder to get analysisresults
    load()
      // get analysisresult record with same dataset date and tags (i.e. result key)
      //.withTagValues(resultKey.tags)
      .get()
      .find(_.resultKey == resultKey)
      // extract analyzer context only
      .map(_.analyzerContext)
  }

  /** Get a builder class to construct a loading query to get AnalysisResults */
  override def load(): MetricsRepositoryMultipleResultsLoader = new MastroMetricsRepositoryMultipleResultsLoader(session, endpoint)
}

// companion object
object MastroMetricsRepository {
  val CHARSET_NAME = "UTF-8"
  val READ_TIMEOUT = 10000

  def apply(session: SparkSession, endpoint: String): MastroMetricsRepository = {
    new MastroMetricsRepository(session, endpoint)
  }

  private[mastro] def putToMastro(session: SparkSession,
                                  endpoint: String,
                                  analysisResult : AnalysisResult) : (Int, String) = {

    val mastroResult = MastroSerde.serialize(
      MetricSet(
        name = "",
        version = "",
        description = "",
        labels = analysisResult.resultKey.tags,
        metrics = List(analysisResult)
      )
    )

    val response = Http(endpoint)
      .put(mastroResult)
      .header("Content-Type", "application/json")
      .header("Charset", CHARSET_NAME)
      .option(HttpOptions.readTimeout(READ_TIMEOUT))
      .asString

    (response.code, response.body)
  }

  private[mastro] def getFromMastro(session: SparkSession,
                                       endpoint: String,
                                       tagValues: Option[Map[String, String]]
                                      ): Option[String] = {
    // get on endpoint using the tags as parameters
    val response = Http(url = endpoint)
        .params(tagValues.getOrElse(Map.empty).toSeq)
        .header("Charset", CHARSET_NAME)
        .option(HttpOptions.readTimeout(READ_TIMEOUT))
        .asString

    if(response.isSuccess) Some(response.body)
    else None
  }
}