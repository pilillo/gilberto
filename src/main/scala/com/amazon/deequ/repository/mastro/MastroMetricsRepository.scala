package com.amazon.deequ.repository.mastro

import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.repository.{AnalysisResult, MetricsRepository, MetricsRepositoryMultipleResultsLoader, ResultKey}
import io.lemonlabs.uri.Url
import org.apache.spark.sql.SparkSession
import scalaj.http.{Http, HttpOptions}

import scala.collection.Map

class MastroMetricsRepository (session: SparkSession, endpoint: String, metricSetInfo : String) extends MetricsRepository{
  override def save(resultKey: ResultKey, analyzerContext: AnalyzerContext): Unit = {
    val successfulMetrics = analyzerContext.metricMap
      .filter { case (_, metric) => metric.value.isSuccess }

    val analyzerContextWithSuccessfulValues = AnalyzerContext(successfulMetrics)
    val result = AnalysisResult(resultKey, analyzerContextWithSuccessfulValues)

    // put metric to mastro
    MastroMetricsRepository.putToMastro(session, endpoint, metricSetInfo, result)
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
  override def load(): MetricsRepositoryMultipleResultsLoader = new MastroMetricsRepositoryMultipleResultsLoader(session, endpoint, metricSetInfo)
}

// companion object
object MastroMetricsRepository {
  val CHARSET_NAME = "UTF-8"
  val READ_TIMEOUT = 10000
  val DEFAULT_METRIC_SET_NAME = "Gilberto"
  val SPLITTING_SEP = ":"

  def apply(session: SparkSession, endpoint: String, metricSetInfo : String): MastroMetricsRepository = {
    new MastroMetricsRepository(session, endpoint, metricSetInfo)
  }

  private[mastro] def getMetricSetInfo(metricSetInfo: String) : (String, String, String) = {
    // of kind name:version
    metricSetInfo.split(SPLITTING_SEP) match {
      case Array(name, version, description, _*) => (name, version, description)
      case Array(name, version) => (name, version, "")
      case _ => (metricSetInfo, "", "")
    }
  }

  private[mastro] def putToMastro(session: SparkSession,
                                  endpoint: String,
                                  metricSetInfo : String,
                                  analysisResult : AnalysisResult) : (Int, String) = {

    val (name, version, description) = getMetricSetInfo(metricSetInfo)

    val mastroResult = MastroSerde.serialize(
      MetricSet(
        // use metricSetInfo if provided from CLI or a DEFAULT_METRIC_SET_NAME otherwise
        name = if(name.isEmpty) DEFAULT_METRIC_SET_NAME else name,
        // set version, if provided, else empty
        version = version,
        // set description, if provided, else empty
        description = description,
        // use tags as labels
        labels = analysisResult.resultKey.tags,
        // the metrics are the entire AnalysisResult of the current run
        // todo: why are we using a list if we add every time a new metricset?
        metrics = List(analysisResult)
      )
    )

    // Performing a PUT to endpoint, e.g. http://host:port/metricstore/
    // https://github.com/data-mill-cloud/mastro/tree/master/metricstore
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
                                    metricSetInfo : String,
                                    tagValues: Option[Map[String, String]]
                                   ): Option[String] = {
    // the name is not a unique id but a field that can be used to group featureset of the same pipeline
    // this means, we can directly search by name if a metricSetInfo is passed
    // alternatively, we can query by tagValues and retrieve all metricSets with those tags
    // we can refine the search by either including the metricSet name in the tags
    // or by retrieving all and afterwards filter only those with the specific metricSet name of interest
    val baseUrl = Url.parse(endpoint)

    // if a name is specified, use it to retrieve all metricsets with that name
    val (name, _, _) = getMetricSetInfo(metricSetInfo)

    val response = if(name.isEmpty) {
      // use tags
      Http(url = baseUrl.toString)
        .params(tagValues.getOrElse(Map.empty).toSeq)
        .header("Charset", CHARSET_NAME)
        .option(HttpOptions.readTimeout(READ_TIMEOUT))
        .asString
    }else{
      // use name
      // construct something of kind protocol://host:port/service/:name e.g., http://metadata-metricstore:8085/metricstore/gilberto
      // https://github.com/lemonlabsuk/scala-uri#url-percent-encoding
      val getByName = baseUrl.addPathPart("name").addPathPart(name).toString

      Http(url = getByName)
        .header("Charset", CHARSET_NAME)
        .option(HttpOptions.readTimeout(READ_TIMEOUT))
        .asString
    }

    if(response.isSuccess) Some(response.body)
    else None
  }
}