package com.github.pilillo

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.github.pilillo.Helpers._
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DataframeGenerator}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class DataFrameCheck extends FunSuite with DataFrameSuiteBase with Checkers {

  test("assert dataframes generated correctly") {
    val schema = StructType(List(StructField("name", StringType), StructField("age", IntegerType)))

    val dataframeGen = DataframeGenerator.arbitraryDataFrame(spark.sqlContext, schema)

    val property =
      forAll(dataframeGen.arbitrary) {
        dataframe => dataframe.schema === schema && dataframe.count >= 0
      }

    check(property)
  }

  test("test implicit class import and run") {
    import spark.sqlContext.implicits._

    val df = spark.sparkContext.parallelize(List(1, 2, 3)).toDF

    assert(df.verify().hasPassedValidation())
  }

  case class Item(id: Long, productName: String, description: String, priority: String, numViews: Long)

  test("test deequ default") {
    val rdd = spark.sparkContext.parallelize(Seq(
      Item(1, "Thingy A", "awesome thing.", "high", 0),
      Item(2, "Thingy B", "available at http://thingb.com", null, 0),
      Item(3, null, null, "low", 5),
      Item(4, "Thingy D", "checkout https://thingd.ca", "low", 10),
      Item(5, "Thingy E", null, "high", 12)))

    val data = spark.createDataFrame(rdd)

    /*
     Applying the following checks from the deequ example:
      - there are 5 rows in total
      - values of the id attribute are never NULL and unique
      - values of the productName attribute are never NULL
      - the priority attribute can only contain "high" or "low" as value
      - numViews should not contain negative values
      - at least half of the values in description should contain a url
      - the median of numViews should be less than or equal to 10
     */

    val valResult = data
      .addCheck(
        Check(CheckLevel.Error, "unit testing my data")
          .hasSize(_ == 5) // we expect 5 rows
          .isComplete("id") // should never be NULL
          .isUnique("id") // should not contain duplicates
          .isComplete("productName") // should never be NULL
          // should only contain the values "high" and "low"
          .isContainedIn("priority", Array("high", "low"))
          .isNonNegative("numViews") // should not contain negative values
          // at least half of the descriptions should contain a url
          .containsURL("description", _ >= 0.5)
          // half of the items should have less than 10 views
          .hasApproxQuantile("numViews", 0.5, _ <= 10)
      ).verify()

    assert(valResult.hasPassedValidation())
  }
}
