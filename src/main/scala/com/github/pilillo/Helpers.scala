package com.github.pilillo

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckStatus}
import org.apache.spark.sql.DataFrame
import com.amazon.deequ.constraints._
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import org.apache.log4j.Logger

object Helpers {

 implicit class ValidatedDataFrame(df : DataFrame) {

  implicit val verifier = VerificationSuite()
  implicit val verificationRunBuilder = verifier.onData(df)

  def addCheck(check : Check) : DataFrame = {
   verificationRunBuilder.addCheck(check)
   df
  }

  def addChecks(check : Seq[Check]) : DataFrame = {
   verificationRunBuilder.addChecks(check)
   df
  }

  def persistMetrics(repositoryPath : String, tags : Map[String,String]) : DataFrame = {
   // https://github.com/awslabs/deequ/blob/master/src/main/scala/com/amazon/deequ/examples/metrics_repository_example.md
   val resultKey = ResultKey(System.currentTimeMillis(), tags)
   verificationRunBuilder
     .useRepository(FileSystemMetricsRepository(df.sparkSession, repositoryPath))
     .saveOrAppendResult(resultKey)
   df
  }

  def verify() : VerificationResult = {
   verificationRunBuilder.run()
  }
 }

 implicit class VerificationResultInspector(vr : VerificationResult) {

  val log : Logger = Logger.getLogger(this.getClass.getName)

  def hasPassedValidation() : Boolean = {
   vr.status == CheckStatus.Success
  }

  def getResult() : Iterable[ConstraintResult] = {
   vr.checkResults.flatMap{ case (_, checkResult) => checkResult.constraintResults }
  }

  def getViolatedConstraints() : Iterable[ConstraintResult] = {
   vr.getResult().filter{ _.status != ConstraintStatus.Success }
  }

  def logViolatedConstraints() : Unit = {
   vr.getViolatedConstraints().foreach{
    result => log.info(s"${result.constraint}: ${result.message.get}")
   }
  }
 }



}
