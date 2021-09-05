package com.github.pilillo

import com.amazon.deequ.examples.Item
import com.github.pilillo.commons.TimeInterval
import com.github.pilillo.pipelines.BatchValidator._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class ValidatorTest extends FunSuite with DataFrameSuiteBase with Checkers {

  test("validator"){
    import spark.implicits._

    val data = spark.sparkContext.parallelize(
      Seq(
        (1, "Thingy A", "awesome thing.", "high", 0),
        (2, "Thingy B", "available at http://thingb.com", null, 0),
        (3, null, null, "low", 5),
        (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        (5, "Thingy E", null, "high", 12))
      )
      .toDF("id", "productName", "description", "priority", "numViews")

    val result = data.validate()
    println("Result:", result)
  }

}
