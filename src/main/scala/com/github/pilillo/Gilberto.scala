package com.github.pilillo

import com.github.pilillo.commons.{TimeInterval, TimeIntervalArguments, Utils}
import org.apache.log4j.Logger
import Helpers._
import com.github.pilillo.pipelines.BatchAnomalyDetector._
import com.github.pilillo.Settings.{Configs, Formats}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.github.pilillo.pipelines.BatchValidator._
import com.github.pilillo.pipelines.BatchSuggester._
import com.github.pilillo.pipelines.BatchProfiler._

import java.nio.file.Paths
import scala.io.Source
import scala.util.{Failure, Success, Try}

object Gilberto {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val arguments = TimeInterval.parse(args)
    if (arguments.isEmpty) {
      log.error("Invalid arguments")
      sys.exit(4)
    }
    log.info(s"Running ${arguments.get.action}")
    val spark: SparkSession = SparkJob.get(s"Gilberto-${args(0)}")
    val input = spark.table(arguments.get.source).whereTimeIn(arguments.get.dateFrom, arguments.get.dateTo)
    input match {
      case Failure(e) => {
        log.error(e)
        sys.exit(4)
      }
      case Success(df) => {
        // write results to destination/partition=value/

        val exitCode = arguments.get.action match {
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
              sys.exit(5)
            }
            val codeConfig = df.loadCodeConfig(arguments.get.codeConfigPath)
            df.validate(codeConfig, arguments.get.repository)
          }
          case "detect" => {
            if (arguments.get.codeConfigPath == null) {
              log.error("No path provided for code config")
              sys.exit(5)
            }
            val codeConfig = df.loadCodeConfig(arguments.get.codeConfigPath)
            df.detect(codeConfig, arguments.get.repository)
          }
          case _ => {
            log.error(s"No pipeline named ${arguments.get.action}")
            2
          }
        }
        if (exitCode != 0) sys.exit(exitCode)
      }
    }
  }

}
