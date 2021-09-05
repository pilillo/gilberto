package com.amazon.deequ.repository.mastro

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultSerde, AnalysisResultSerializer, AnalyzerSerializer, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import org.apache.spark.sql.SparkSession

class MastroMetricsRepository (session: SparkSession, endpoint: String) extends MetricsRepository{
  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    // select only the successfull metrics
    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)

    val result = AnalysisResult(resultKey, analyzerContextWithSuccessfulValues)

    // put metric to mastro
    MastroMetricsRepository.putToMastro(session, endpoint,
      //analyzerContextWithSuccessfulValues
      result
    )
  }

  override def loadByKey(resultKey: ResultKey): Option[AnalyzerContext] = {
    /*
    Option(
      resultsRepository.get(resultKey)

    ).map { _.analyzerContext }
     */
    load()
      // todo: add search - e.g. by tags

      .get().find(_.resultKey == resultKey).map(_.analyzerContext)
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
                                  //analyzerContext: AnalyzerContext
                                  analysisResult : AnalysisResult
                                 )
  : Unit = {
    //val successMetrics = AnalyzerContext.successMetricsAsJson(analyzerContext)
    val result = AnalysisResultSerde.serialize(Seq(analysisResult))

    /* includes both - e.g.
    [
      {"entity":"Dataset","instance":"*","name":"Size","value":5.0},          // meets constraints
      {"entity":"Column","instance":"numViews","name":"Minimum","value":0.0}  // does not meet constraint!
    ]
     */
    //println(successMetrics)
    println(result)
    /*
    val result = Http(endpoint)
      .put(successMetrics)
      .header("Content-Type", "application/json")
      .header("Charset", CHARSET_NAME)
      .option(HttpOptions.readTimeout(READ_TIMEOUT)).asString
    */
  }

  private[mastro] def getFromMastro[T](session: SparkSession,
                                   endpoint: String): Option[T] = {
    None
  }
}