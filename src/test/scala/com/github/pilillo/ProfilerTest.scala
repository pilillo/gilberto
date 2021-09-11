package com.github.pilillo

import com.amazon.deequ.examples.RawData
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import com.github.pilillo.pipelines.BatchProfiler._

class ProfilerTest extends FunSuite with DataFrameSuiteBase with Checkers {

  test("profiler"){
    val rows = spark.sparkContext.parallelize(Seq(
      RawData("thingA", "13.0", "IN_TRANSIT", "true"),
      RawData("thingA", "5", "DELAYED", "false"),
      RawData("thingB", null,  "DELAYED", null),
      RawData("thingC", null, "IN_TRANSIT", "false"),
      RawData("thingD", "1.0",  "DELAYED", "true"),
      RawData("thingC", "7.0", "UNKNOWN", null),
      RawData("thingC", "20", "UNKNOWN", null),
      RawData("thingE", "20", "DELAYED", "false")
    ))
    val rawData = spark.createDataFrame(rows)

    val result = rawData.profile()
    result.show()
  }
}
