package com.github.pilillo.pipelines

import com.amazon.deequ.analyzers.{Analyzer, DoubleValuedState}
import com.amazon.deequ.anomalydetection.AnomalyDetectionStrategy
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.{VerificationResult, VerificationSuite}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object BatchAnomalyDetector {
  val log : Logger = Logger.getLogger(getClass.getName)

  def getAnomalyCheckAnalyzer[S <: DoubleValuedState[_]](codeConfig : String) : (AnomalyDetectionStrategy, Analyzer[S, DoubleMetric]) = {
    val toolbox = currentMirror.mkToolBox()
    //val toolbox = reflect.runtime.universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val ast = toolbox.parse(codeConfig)

    toolbox.eval(ast).asInstanceOf[(AnomalyDetectionStrategy, Analyzer[S, DoubleMetric])]
  }

  def logResult(verificationResult : VerificationResult, errorLevel : Boolean) : Unit = {
    verificationResult.checkResults
      .flatMap { case (_, checkResult) => checkResult.constraintResults }
      // get all failed constraints
      .filter { _.status != ConstraintStatus.Success }
      .foreach { result =>
        val message = s"${result.constraint}: ${result.message.get}"
        if(errorLevel) log.error(message) else log.warn(message)
      }
  }

  implicit class Detector(df: DataFrame) {
    def detect(codeConfig : String,
               repository : MetricsRepository,
               resultKey : ResultKey = ResultKey(System.currentTimeMillis(), Map.empty)
              ): Int = {

      val (detectionStrategy, analyzer) = getAnomalyCheckAnalyzer(codeConfig)
      val verificationResult = VerificationSuite()
        .onData(df)
        .useRepository(repository)
        .saveOrAppendResult(resultKey)
        .addAnomalyCheck(detectionStrategy, analyzer)
        .run()

      verificationResult.status match {
       case CheckStatus.Success => {
         log.info("The data passed the anomaly check, everything is fine!")
         0
       }
       case CheckStatus.Warning => {
         logResult(verificationResult, false)
         1
       }
       case CheckStatus.Error => {
         logResult(verificationResult, true)
         2
       }
      }
    }
  }
}
