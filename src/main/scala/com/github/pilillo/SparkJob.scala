package com.github.pilillo

import org.apache.spark.sql.SparkSession

case object SparkJob {
  def get(appName : String) : SparkSession = {
    val spark = SparkSession.builder.appName(appName).getOrCreate()
    spark
  }
}
