package com.github.pilillo

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.github.pilillo.Helpers._
import com.github.pilillo.Settings.ColNames
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

  test("test df") {
    import spark.implicits._
    val data = spark.sparkContext.parallelize(
      Seq(
        (2021, 2, 15, "Item1"),
        (2021, 1, 11, "Item2"),
        (2020, 12, 12, "Item3"),
        (2020, 11, 30, "Item4"),
        (2019, 12, 31, "Item5")
      )
    ).toDF(ColNames.YEAR, ColNames.MONTH, ColNames.DAY, "mycol")

    val expected = spark.sparkContext.parallelize(
      Seq(
        (2021, 1, 11, "Item2"),
        (2020, 12, 12, "Item3"),
        (2020, 11, 30, "Item4")
      )
    ).toDF(ColNames.YEAR, ColNames.MONTH, ColNames.DAY, "mycol")

    assertDataFrameNoOrderEquals(
      expected,
      data.whereTimeIn("2020-01-01", "2021-01-11").get
    )
  }

  test("all elems contained"){
    val partitionedResult = Seq("mycol", "year", "month", "day")
    val targetCols = Seq("year")

    // there is a column in target that is not available in partitioned? NO - so target - partitioned is empty
    assert(
      targetCols.filterNot(partitionedResult.toSet).isEmpty
    )

    // append unavailable col to target
    val targetCols2 = targetCols ++ Seq("wrong_col")
    // yes - so target - partitioned is not empty
    assert(
      !targetCols2.filterNot(partitionedResult.toSet).isEmpty
    )
    // diff = { wrong_col } so it is not empty
  }


}
