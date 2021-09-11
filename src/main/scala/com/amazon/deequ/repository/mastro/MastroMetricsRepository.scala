package com.amazon.deequ.repository.mastro

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultSerde, AnalysisResultSerializer, AnalyzerSerializer, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}

import org.apache.spark.sql.SparkSession
import scalaj.http.{Http, HttpOptions}

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
                                  analysisResult : AnalysisResult) : (Int, String) = {
    //val successMetrics = AnalyzerContext.successMetricsAsJson(analyzerContext)
    //println(successMetrics)
    val serializedAnalysisResult = AnalysisResultSerde.serialize(Seq(analysisResult))
    //println(serializedAnalysisResult)
    /*   [
          {
            "resultKey": {
              "dataSetDate": 1630876393300,
              "tags": {}
            },
            "analyzerContext": {
              "metricMap": [
                {
                  "analyzer": {
                    "analyzerName": "Size"
                  },
                  "metric": {
                    "metricName": "DoubleMetric",
                    "entity": "Dataset",
                    "instance": "*",
                    "name": "Size",
                    "value": 5.0
                  }
                },
                {
                  "analyzer": {
                    "analyzerName": "Minimum",
                    "column": "numViews"
                  },
                  "metric": {
                    "metricName": "DoubleMetric",
                    "entity": "Column",
                    "instance": "numViews",
                    "name": "Minimum",
                    "value": 0.0
                  }
                }
              ]
            }
          }
        ] */

    val response = Http(endpoint)
      .put(serializedAnalysisResult)
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
    /*
    val gson = new GsonBuilder().setPrettyPrinting().create()
    val jsonVal = gson.toJson(tagValues)
    */

    // get on endpoint using the tags as parameters
    val response = Http(url = endpoint)
        .params(tagValues.getOrElse(Map.empty))
        .header("Charset", CHARSET_NAME)
        .option(HttpOptions.readTimeout(READ_TIMEOUT))
        .asString

    if(response.isSuccess) Some(response.body)
    else None
  }
}