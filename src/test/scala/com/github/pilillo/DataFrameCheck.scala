package com.github.pilillo

import com.holdenkarau.spark.testing.{DataframeGenerator, SharedSparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import com.github.pilillo.Helpers._

class DataFrameCheck extends FunSuite with SharedSparkContext with Checkers {

  test("assert dataframes generated correctly") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    //val sqlContext = new SQLContext(sc)

    val dataframeGen = DataframeGenerator.arbitraryDataFrame(spark.sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema && dataframe.count >= 0
      }

    check(property)
  }

  test("test implicit class import and run") {
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.sqlContext.implicits._
    val df = sc.parallelize(List(1, 2, 3)).toDF

    assert(df.verify().hasPassedValidation())
  }
}
