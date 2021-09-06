package com.amazon.deequ.repository.querable

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.runners.AnalyzerContext
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{AnalysisResult, AnalysisResultSerde, MetricsRepositoryMultipleResultsLoader}
import org.apache.spark.sql.SparkSession

class QuerableMetricsMultipleResultsLoader (session: SparkSession, path: String)
  extends MetricsRepositoryMultipleResultsLoader {

  private[this] var tagValues: Option[Map[String, String]] = None
  private[this] var forAnalyzers: Option[Seq[Analyzer[_, Metric[_]]]] = None
  private[this] var before: Option[Long] = None
  private[this] var after: Option[Long] = None

  /**
   * Filter out results that don't have specific values for specific tags
   *
   * @param tagValues Map with tag names and the corresponding values to filter for
   */
  override def withTagValues(tagValues: Map[String, String]): MetricsRepositoryMultipleResultsLoader = {
    this.tagValues = Option(tagValues)
    this
  }

  /**
   * Choose all metrics that you want to load
   *
   * @param analyzers A sequence of analyers who's resulting metrics you want to load
   */
  override def forAnalyzers(analyzers: Seq[Analyzer[_, Metric[_]]]): MetricsRepositoryMultipleResultsLoader = {
    this.forAnalyzers = Option(analyzers)
    this
  }

  /**
   * Only look at AnalysisResults with a result key with a smaller value
   *
   * @param dateTime The maximum dateTime of AnalysisResults to look at
   */
  override def before(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.before = Option(dateTime)
    this
  }

  /**
   * Only look at AnalysisResults with a result key with a greater value
   *
   * @param dateTime The minimum dateTime of AnalysisResults to look at
   */
  override def after(dateTime: Long): MetricsRepositoryMultipleResultsLoader = {
    this.after = Option(dateTime)
    this
  }

  /**
   * Get the AnalysisResult
   */
  override def get(): Seq[AnalysisResult] = {
    // 1. load results from repository
    val results = QuerableMetricsRepository
      .read(session, path, tagValues)
      // deserialize content from string result
      .map { content => AnalysisResultSerde.deserialize(content) }
      .getOrElse(Seq.empty)

    // 2. enforce predicates to select only certain results
    results
      .filter { result => after.isEmpty || after.get <= result.resultKey.dataSetDate }
      .filter { result => before.isEmpty || result.resultKey.dataSetDate <= before.get }
      .filter { result =>
        tagValues.isEmpty ||
          tagValues.get.toSet.subsetOf(result.resultKey.tags.toSet)
      }
      .map { analysisResult =>
        val requestedMetrics = analysisResult
          .analyzerContext
          .metricMap
          .filterKeys(analyzer => forAnalyzers.isEmpty || forAnalyzers.get.contains(analyzer))
        AnalysisResult(analysisResult.resultKey, AnalyzerContext(requestedMetrics))
      }.toSeq
  }
}
