package com.github.pilillo

import com.amazon.deequ.checks.CheckLevel
import com.github.pilillo.pipelines.BatchValidator._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class ValidatorTest extends FunSuite with DataFrameSuiteBase with Checkers {

  test("parse code config"){

    /* Need to import the deequ libs to avoid:
    not found: value Check
      scala.tools.reflect.ToolBoxError: reflective compilation has failed:
    */
    val codeConfig = """
           import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
           Seq(
            Check(CheckLevel.Error, "data testing with error level")
              .hasSize(_ >0)
              .hasMin("numViews", _ > 12)
           )
         """
    val checks = getChecks(codeConfig)
    // 1 check with 2 constraints is defined - the check level is error
    assert(1, checks.length)
    assert(CheckLevel.Error, checks(0).level)
  }

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

    // CORRECT CHECK
    var codeConfig = """
           import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
           Seq(
            Check(CheckLevel.Error, "data testing with error level")
              .hasSize(_ >0)
              .hasMin("numViews", _ <= 12)
           )
         """

    var result = data.validate(
      codeConfig,
      //"http://localhost",
      "myrepo"
    )
    assert(0, result)

    // FAILING CHECK
    codeConfig = """
           import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
           Seq(
            Check(CheckLevel.Error, "unit testing my data")
              .hasSize(_ >0)
              .hasMin("numViews", _ > 12)
           )
         """
    result = data.validate(
      codeConfig,
      "myrepo"
      //"http://localhost",
    )
    assert(4, result)
  }
}
