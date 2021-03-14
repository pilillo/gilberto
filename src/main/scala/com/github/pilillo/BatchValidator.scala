package com.github.pilillo

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import Helpers._

class BatchValidator extends Validator[DataFrame] {
  val spark : SparkSession = SparkSession.builder().appName("BatchValidator").getOrCreate()
  val log : Logger = Logger.getLogger(getClass.getName)

  override def open(): DataFrame = {
    spark.emptyDataFrame
  }

  override def validate(): Boolean = {
    // validate opened resource
    true
  }
}
