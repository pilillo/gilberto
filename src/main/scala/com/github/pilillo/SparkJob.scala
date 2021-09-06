package com.github.pilillo

import org.apache.spark.sql.SparkSession

case object SparkJob {
  def get(appName : String) : SparkSession = {
    val spark = SparkSession.builder.appName(appName).getOrCreate()
    // from 2.3.x on - enable dynamic partitions - default is static
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark
  }
}
