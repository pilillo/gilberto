package com.github.pilillo.pipelines

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.pilillo.Helpers._
import com.github.pilillo.SparkJob
import com.github.pilillo.commons.{TimeInterval, TimeIntervalArguments, Utils}

object BatchValidator {
  val log : Logger = Logger.getLogger(getClass.getName)
  implicit class Validator(df: DataFrame) {
    def validate(): Int = {
      // todo: extract checks from smaller period, then use it on longer period
      val repositoryPath = "myrepo.json"
      val tags = Map[String,String]()
      val resultKey = ResultKey(System.currentTimeMillis(), tags)
      val verificationResult = VerificationSuite()
        .onData(df)
        .addCheck(
          Check(CheckLevel.Error, "unit testing my data")
            // todo: move to config file
            .hasSize(_ >0)
        )
        .useRepository(FileSystemMetricsRepository(df.sparkSession, repositoryPath))
        .saveOrAppendResult(resultKey)
        .run()

      if (verificationResult.hasPassedValidation()) {
        log.info("The data passed the test, everything is fine!")
        0
      } else {
        log.error("We found errors in the data:\n")

        val resultsForAllConstraints = verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }

        resultsForAllConstraints
          .filter { _.status != ConstraintStatus.Success }
          .foreach { result => println(s"${result.constraint}: ${result.message.get}") }

        4
      }
    }
  }
}
