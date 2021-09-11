package com.github.pilillo

import com.amazon.deequ.VerificationResult
import com.amazon.deequ.checks.CheckStatus
import com.amazon.deequ.constraints._
import com.amazon.deequ.repository.MetricsRepository
import com.amazon.deequ.repository.mastro.MastroMetricsRepository
import com.amazon.deequ.repository.querable.QuerableMetricsRepository
import com.github.pilillo.Settings.{ColNames, Formats}
import com.github.pilillo.commons.Utils
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.time.{LocalDate, ZoneId}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object Helpers {

 implicit class DfUtils(df : DataFrame) {

  def whereTimeIn(dateFrom : String, dateTo : String) : Try[DataFrame] = {
   val (from, to) = Utils.parseTimeRange(dateFrom, dateTo)
   if(from.isFailure || to.isFailure){
    Failure(new java.lang.NumberFormatException("Wrong format for provided input date"))
   }
   Success(df.whereTimeIn(from.get, to.get))
  }

  def whereTimeIn(from : LocalDate, to : LocalDate) : DataFrame = {
   val res = df
     // concat cols to create date col
     .withColumn(
      ColNames.DATE_FILTER_UTC,
      format_string("%04d%s%02d%s%02d", col(ColNames.YEAR), lit(Formats.dateDelimiter), col(ColNames.MONTH), lit(Formats.dateDelimiter), col(ColNames.DAY))
     )
     // convert to date
     .withColumn(ColNames.DATE_FILTER_UTC, date_format(col(ColNames.DATE_FILTER_UTC), Formats.inputDateFormat))
     // force conversion to UTC - Spark assumes by default the system zone
     .withColumn(ColNames.DATE_FILTER_UTC, from_utc_timestamp(col(ColNames.DATE_FILTER_UTC), ZoneId.systemDefault.toString).cast("long"))
     // filter data by date interval using the same dateFormatter
     .filter(col(ColNames.DATE_FILTER_UTC).between(from.atStartOfDay(Formats.zoneId).toEpochSecond, to.atStartOfDay(Formats.zoneId).toEpochSecond))
     // drop date column
     .drop(ColNames.DATE_FILTER_UTC)
   res
  }

  /**
   * Version dataframe by current datetime
   * @return
   */
  def version() : DataFrame = {
   val currentDateTime = Utils.getCurrentDateTime()
   df.withColumn(ColNames.VERSION, lit(Utils.formatDate(currentDateTime)))
  }

  val protocols = List("hdfs://", "s3://", "s3a://")

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

  def getRepository(repository : String) : MetricsRepository = {
   // if a valid url is provided, use the mastro repo - otherwise save to file system
   if(Utils.urlValidator.isValid(repository)){
    MastroMetricsRepository(df.sparkSession, endpoint = repository)
   }else{
    //FileSystemMetricsRepository(df.sparkSession, metricsRepo)
    QuerableMetricsRepository(df.sparkSession, path = repository)
   }
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
