package com.github.pilillo.pipelines

import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.Check
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.repository.ResultKey
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.mastro.MastroMetricsRepository
import com.amazon.deequ.repository.querable.QuerableMetricsRepository
import com.github.pilillo.Gilberto.protocols
import com.github.pilillo.Helpers._
import com.github.pilillo.commons.TimeIntervalArguments
import org.apache.commons.validator.routines.UrlValidator
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.io.Source
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

object BatchValidator {
  val log : Logger = Logger.getLogger(getClass.getName)
  val schemes = Array("http","https")
  val urlValidator = new UrlValidator(schemes, null, UrlValidator.ALLOW_LOCAL_URLS)

  def getChecks(codeConfig : String) : Seq[Check] = {
    val toolbox = currentMirror.mkToolBox()
    //val toolbox = reflect.runtime.universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val ast = toolbox.parse(codeConfig)

    toolbox.eval(ast).asInstanceOf[Seq[Check]]
  }

  implicit class Validator(df: DataFrame) {

    def loadCodeConfig(codeConfigPath : String) : String = {
      // check if path contains any of those in the protocols list, if so use spark to load the file
      if (protocols.exists(codeConfigPath.contains(_))) {
        // load from remote FS - e.g. on an hadoop FS
        df.sparkSession
          //.read.text(arguments.get.codeConfigPath)
          .sparkContext.textFile(codeConfigPath)
          .collect().mkString("\n")
      } else {
        // otherwise load as local file - e.g. on k8s we can mount a volume
        Source.fromFile(codeConfigPath).getLines().mkString("\n")
      }
    }

    def validate(codeConfig : String, repository : String): Int = {

      // todo: use current date in tags?
      val tags = Map[String,String]()
      val resultKey = ResultKey(System.currentTimeMillis(), tags)

      val verifier = VerificationSuite()
        .onData(df)
        .addChecks(
          getChecks(codeConfig)
        )

      val verifierWithRepo = if(repository == null || repository.isEmpty){
        verifier
      } else {
        // if a valid url is provided, use the mastro repo - otherwise save to file system
        val repo = if(urlValidator.isValid(repository)){
          MastroMetricsRepository(df.sparkSession, endpoint = repository)
        }else{
          //FileSystemMetricsRepository(df.sparkSession, metricsRepo)
          QuerableMetricsRepository(df.sparkSession, path = repository)
        }
        verifier.useRepository(repo).saveOrAppendResult(resultKey)
      }

      // run verification
      val verificationResult = verifierWithRepo.run()

      if (verificationResult.hasPassedValidation()) {
        log.info("The data passed the test, everything is fine!")
        0
      } else {

        val resultsForAllConstraints = verificationResult.checkResults
          .flatMap { case (_, checkResult) => checkResult.constraintResults }

        // get all failed constraints
        resultsForAllConstraints
          .filter { _.status != ConstraintStatus.Success }
          .foreach { result => log.error(s"${result.constraint}: ${result.message.get}") }

        4
      }
    }
  }
}
