package com.github.pilillo

import com.github.pilillo.commons.{TimeInterval, TimeIntervalArguments, Utils}
import org.apache.log4j.Logger
import Helpers._
import com.github.pilillo.Settings.Configs
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.pilillo.pipelines.BatchValidator._
import com.github.pilillo.pipelines.BatchSuggester._
import com.github.pilillo.pipelines.BatchProfiler._

import scala.util.{Failure, Success, Try}

object Gilberto {
  val log : Logger = Logger.getLogger(getClass.getName)

  def main(args : Array[String]) : Unit = {
    if(args.length == 0 || args(0).isEmpty) {
      sys.exit(5)
    }else{
      log.info(s"Running ${args(0)}")
      val arguments = TimeInterval.parse(args)
      if(arguments.isEmpty){
        log.error("Invalid arguments")
        sys.exit(4)
      }

      val spark : SparkSession = SparkJob.get(s"Gilberto-${args(0)}")
      val input = spark.table(arguments.get.source).whereTimeIn(arguments.get.dateFrom, arguments.get.dateTo)
      input match {
        case Failure(e) => {
          log.error(e)
          sys.exit(4)
        }
        case Success(df) => {
          val exitCode = args(0) match {
            case "profile" => {
              val result = df.profile()
              result.coalesce(Configs.NUM_TARGET_PARTITIONS).write.mode("overwrite").parquet(arguments.get.destination)
              0
            }
            case "suggest" => {
              val result = df.suggest()
              result.coalesce(Configs.NUM_TARGET_PARTITIONS).write.mode("overwrite").parquet(arguments.get.destination)
              0
            }
            case "validate" => {
              df.validate()
            }
            case _ => {
              log.error(s"No pipeline named ${args(0)}")
              2
            }
          }
          if (exitCode != 0) sys.exit(exitCode)
        }
      }
    }
  }
}
