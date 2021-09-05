package com.github.pilillo

import com.github.pilillo.commons.TimeInterval
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import com.github.pilillo.pipelines.BatchSuggester._

class SuggesterTest extends FunSuite with DataFrameSuiteBase with Checkers {

  test("suggester"){
    import spark.implicits._

    val data = spark.sparkContext.parallelize(Seq(
      ("thingA", "13.0", "IN_TRANSIT", "true"),
      ("thingA", "5", "DELAYED", "false"),
      ("thingB", null, "DELAYED", null),
      ("thingC", null, "IN_TRANSIT", "false"),
      ("thingD", "1.0", "DELAYED", "true"),
      ("thingC", "7.0", "UNKNOWN", null),
      ("thingC", "24", "UNKNOWN", null),
      ("thingE", "20", "DELAYED", "false"),
      ("thingA", "13.0", "IN_TRANSIT", "true"),
      ("thingA", "5", "DELAYED", "false"),
      ("thingB", null, "DELAYED", null),
      ("thingC", null, "IN_TRANSIT", "false"),
      ("thingD", "1.0", "DELAYED", "true"),
      ("thingC", "17.0", "UNKNOWN", null),
      ("thingC", "22", "UNKNOWN", null),
      ("thingE", "23", "DELAYED", "false")
    )).toDF("productName", "totalNumber", "status", "valuable")

    val arguments = TimeInterval.parse(Array[String]("--source", "b", "--destination", "c", "--from", "01/01/2020", "--to", "01/01/2020"))
    val result = data.suggest()(spark)
    result.show()
  }

}
