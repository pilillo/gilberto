package com.github.pilillo

import com.amazon.deequ.repository.ResultKey
import com.github.pilillo.Helpers._
import com.github.pilillo.Settings.{ColNames, Formats}
import com.github.pilillo.commons.Utils.{convertToLocalDateTimeViaInstant, getCurrentDateTime, parseTimeRange}
import com.github.pilillo.commons.{TimeInterval, TimeIntervalArguments, Utils}
import com.github.pilillo.pipelines.BatchAnomalyDetector._
import com.github.pilillo.pipelines.BatchProfiler._
import com.github.pilillo.pipelines.BatchSuggester._
import com.github.pilillo.pipelines.BatchValidator._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success}

object Gilberto {
  val log: Logger = Logger.getLogger(getClass.getName)

  def getResultKey(arguments : TimeIntervalArguments) : ResultKey = {
    val dateTime = getCurrentDateTime()
    val processingDate = convertToLocalDateTimeViaInstant(dateTime)
    val (from, to) = parseTimeRange(arguments.dateFrom, arguments.dateTo)

    // ListMap implements an immutable map using a list-based data structure, and thus preserves insertion order.
    val targetCols = if(arguments.partitionBy != null){
      arguments.partitionBy.split(Formats.PARTITIONBY_SPLIT_CHAR)
    }else{
      Array.empty[String]
    }

    val tags = targetCols.toList.map(
      col => col match {
        // interval start date
        case ColNames.START_YEAR => (col, "%04d".format(from.get.getYear))
        case ColNames.START_MONTH => (col, "%02d".format(from.get.getMonthValue))
        case ColNames.START_DAY => (col, "%02d".format(from.get.getDayOfMonth))
        // interval end date
        case ColNames.END_YEAR => (col, "%04d".format(to.get.getYear))
        case ColNames.END_MONTH => (col, "%02d".format(to.get.getMonthValue))
        case ColNames.END_DAY => (col, "%02d".format(to.get.getDayOfMonth))
        // processing time - date of running the pipeline
        case ColNames.PROC_YEAR => (col, "%04d".format(processingDate.getYear))
        case ColNames.PROC_MONTH => (col, "%02d".format(processingDate.getMonthValue))
        case ColNames.PROC_DAY => (col, "%02d".format(processingDate.getDayOfMonth))
        // skip uknown ones
        case _ => null
      }
    ).filter(_ != null)

    ResultKey(dateTime.getTime, ListMap[String, String](tags : _*))
  }

  def main(args: Array[String]): Unit = {
    val arguments = TimeInterval.parse(args)
    if (arguments.isEmpty) {
      log.error("Invalid arguments")
      sys.exit(4)
    }
    log.info(s"Running ${arguments.get.action}")
    val spark: SparkSession = SparkJob.get(s"Gilberto-${args(0)}")
    // assuming either Hive tables or paths
    val df = if( spark.catalog.tableExists(arguments.get.source))
      spark.table(arguments.get.source)
    else spark.read.parquet(arguments.get.source)
    // filter input by time interval
    val input = df.whereTimeIn(arguments.get.dateFrom, arguments.get.dateTo)

    val exitCode = input match {
      case Failure(e) => {
        log.error(e)
        4
      }
      case Success(df) => {
        arguments.get.action match {
          case "profile" => {
            val result = df.profile()
            Utils
              .getPartitionedWriter(result, arguments.get)
              .parquet(arguments.get.destination)
            0
          }
          case "suggest" => {
            val result = df.suggest()
            Utils
              .getPartitionedWriter(result, arguments.get)
              .parquet(arguments.get.destination)
            0
          }
          case "validate" => {
            if (arguments.get.codeConfigPath == null) {
              log.error("No path provided for code config")
              5
            }else{
              val codeConfig = df.loadCodeConfig(arguments.get.codeConfigPath)
              val repo = if(arguments.get.repository == null || arguments.get.repository.isEmpty) {
                null
              }else {
                df.getRepository(arguments.get.repository)
              }
              df.validate(codeConfig, repo, getResultKey(arguments.get))
            }
          }
          case "detect" => {
            if (arguments.get.codeConfigPath == null) {
              log.error("No path provided for code config")
              5
            }else{
              val codeConfig = df.loadCodeConfig(arguments.get.codeConfigPath)
              if(arguments.get.repository == null || arguments.get.repository.isEmpty) {
                log.error("No repository specified")
                5
              }else{
                df.detect(codeConfig, df.getRepository(arguments.get.repository), getResultKey(arguments.get))
              }
            }
          }
          case _ => {
            log.error(s"No pipeline named ${arguments.get.action}")
            2
          }
        }
      }
    }
    if (exitCode != 0) sys.exit(exitCode)
  }
}