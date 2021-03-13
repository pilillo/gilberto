package com.github.pilillo

import org.apache.spark.sql.SparkSession

class BatchValidator {
  val spark : SparkSession = SparkSession.builder().appName("").getOrCreate()

  // todo
}
