package com.github.pilillo.pipelines

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.repository.mastro.MastroMetricsRepository
import com.amazon.deequ.repository.querable.QuerableMetricsRepository
import com.github.pilillo.Helpers._
import com.github.pilillo.commons.Utils
import org.apache.commons.validator.routines.UrlValidator
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object BatchValidator {
  val log : Logger = Logger.getLogger(getClass.getName)

  def getChecks(codeConfig : String) : Seq[Check] = {
    val toolbox = currentMirror.mkToolBox()
    //val toolbox = reflect.runtime.universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val ast = toolbox.parse(codeConfig)

    toolbox.eval(ast).asInstanceOf[Seq[Check]]
  }

  implicit class Validator(df: DataFrame) {

    def validate(codeConfig : String,
                 repository : MetricsRepository,
                 resultKey : ResultKey = ResultKey(System.currentTimeMillis(), Map[String,String]())
                ): Int = {

      val verifier = VerificationSuite()
        .onData(df)
        .addChecks(
          getChecks(codeConfig)
        )

      // repository is optional for the data validator
      val verifierWithRepo = if(repository == null) verifier
      else verifier.useRepository(repository).saveOrAppendResult(resultKey)

      // run verification
      val verificationResult = verifierWithRepo.run()

      if (verificationResult.hasPassedValidation()) {
        log.info("The data passed the test, everything is fine!")
        0
      } else {
        verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }
          // get all failed constraints
          .filter { _.status != ConstraintStatus.Success }
          .foreach { result => log.error(s"${result.constraint}: ${result.message.get}") }
        4
      }
    }
  }
}
