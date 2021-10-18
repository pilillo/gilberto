package com.github.pilillo.pipelines
import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile, StandardColumnProfile}
import com.github.pilillo.Settings.ColNames
import com.github.pilillo.commons.Arguments
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.pilillo.Helpers._

object BatchProfiler {
  val log : Logger = Logger.getLogger(getClass.getName)

  def flattenDistribution(distribution : Option[Distribution]) : Option[(Long, Seq[(String, Double, Long)])] = {
    if(distribution.isDefined){
      Some((
          distribution.get.numberOfBins,
          distribution.get.values.map{ case (k, v) => (k, v.ratio, v.absolute)}.toSeq
        ))
    }else{
      None
    }
  }

  implicit class Profiler(df: DataFrame) {
    def profile(): DataFrame = {

      val result = ColumnProfilerRunner()
        .onData(df)
        .run()

      val spark = df.sparkSession
      import spark.implicits._

      // result is a bunch of profiles, each for each data column
      result.profiles.mapValues{
        case NumericColumnProfile(column, completeness, approximateNumDistinctValues, dataType, isDataTypeInferred, typeCounts, histogram, kll, mean, maximum, minimum, sum, stdDev, approxPercentiles) => (
          column, completeness, approximateNumDistinctValues, dataType.toString, isDataTypeInferred, typeCounts,
          flattenDistribution(histogram), // skipping kll
          mean, maximum, minimum, sum, stdDev, approxPercentiles
        )
        case StandardColumnProfile(column, completeness, approximateNumDistinctValues, dataType, isDataTypeInferred, typeCounts, histogram) => (
          column, completeness, approximateNumDistinctValues, dataType.toString, isDataTypeInferred, typeCounts,
          flattenDistribution(histogram), // skipping kll
          None, None, None, None, None, None
        )
      }
        .values
        .toSeq
        .toDF(
          ColNames.COLUMN_NAME,
          ColNames.COMPLETENESS,
          ColNames.APPROX_NUM_DISTINCT_VALS,
          ColNames.DATA_TYPE,
          ColNames.IS_DATATYPE_INFERRED,
          ColNames.TYPE_COUNTS,
          ColNames.HISTOGRAM,
          ColNames.MEAN,
          ColNames.MAXIMUM,
          ColNames.MINIMUM,
          ColNames.SUM,
          ColNames.STD_DEV,
          ColNames.APPROX_PERCENTILES
        )
        .version()
    }
  }
}

