package com.github.pilillo

import com.amazon.deequ.{VerificationResult, VerificationSuite}
import com.amazon.deequ.checks.{Check, CheckStatus}
import org.apache.spark.sql.DataFrame
import com.amazon.deequ.constraints._
import org.apache.log4j.Logger

object Helpers {

 implicit class ValidatedDataFrame(df : DataFrame) {

  implicit val verifier = VerificationSuite()
  implicit val verificationRunBuilder = verifier.onData(df)

  def addCheck(check : Check) : Unit = {
   verificationRunBuilder.addCheck(check)
  }

  def addChecks(check : Seq[Check]) : Unit = {
   verificationRunBuilder.addChecks(check)
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
