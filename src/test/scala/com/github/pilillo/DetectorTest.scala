package com.github.pilillo



import com.amazon.deequ.analyzers.Size
import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.github.pilillo.pipelines.BatchAnomalyDetector.getAnomalyCheckAnalyzer
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class DetectorTest extends FunSuite with DataFrameSuiteBase with Checkers {
  test("parse anomaly check"){
    val codeConfig = """
           import com.amazon.deequ.anomalydetection._
           import com.amazon.deequ.analyzers._
           (
             RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),
             Size()
           )
         """
    val (detectionStrategy, analyzer) = getAnomalyCheckAnalyzer(codeConfig)
    // 1 check with 2 constraints is defined - the check level is error
    assert( detectionStrategy.isInstanceOf[AnomalyDetectionStrategy] )
    assert( analyzer.isInstanceOf[Size] )
  }
}
