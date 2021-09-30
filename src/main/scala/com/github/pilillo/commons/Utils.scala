package com.github.pilillo.commons

import com.github.pilillo.Settings.{ColNames, Configs, Formats}
import org.apache.commons.validator.routines.UrlValidator
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.{Calendar, Date, TimeZone}
import scala.util.Try

object Utils {

    @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

    def getCurrentDateTime() : Date = {
        Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault())).getTime
    }

    def convertToLocalDateTimeViaInstant(dateToConvert: Date): LocalDate = {
        dateToConvert.toInstant.atZone(ZoneId.systemDefault).toLocalDate
    }

    def formatDate(d : Date) : String = {
        //Formats.inputDateFormat.format(d)
        new SimpleDateFormat(Formats.inputDateFormat).format(d)
    }

    def parseTimeRange(from : String, to : String) : (Try[LocalDate], Try[LocalDate]) = {
        (Try(LocalDate.parse(from, Formats.inputDateFormatter)), Try(LocalDate.parse(to, Formats.inputDateFormatter)) )
    }

    def partitionByStandardCols(result : DataFrame, arguments : TimeIntervalArguments) : (DataFrame, Array[String]) = {
        val targetCols = arguments.partitionBy.split(Formats.PARTITIONBY_SPLIT_CHAR)
        val (from, to) = parseTimeRange(arguments.dateFrom, arguments.dateTo)
        val processingDate = convertToLocalDateTimeViaInstant(getCurrentDateTime())

        val partitionedResult = targetCols.foldLeft(result)(
            (df, col) => col match {
                // interval start date
                case ColNames.START_YEAR => df.withColumn(ColNames.START_YEAR, lit("%04d".format(from.get.getYear)))
                case ColNames.START_MONTH => df.withColumn(ColNames.START_MONTH, lit("%02d".format(from.get.getMonthValue)))
                case ColNames.START_DAY => df.withColumn(ColNames.START_DAY, lit("%02d".format(from.get.getDayOfMonth)))
                // interval end date
                case ColNames.END_YEAR => df.withColumn(ColNames.END_YEAR, lit("%04d".format(to.get.getYear)))
                case ColNames.END_MONTH => df.withColumn(ColNames.END_MONTH, lit("%02d".format(to.get.getMonthValue)))
                case ColNames.END_DAY => df.withColumn(ColNames.END_DAY, lit("%02d".format(to.get.getDayOfMonth)))
                // processing time - date of running the pipeline
                case ColNames.PROC_YEAR => df.withColumn(ColNames.PROC_YEAR, lit("%04d".format(processingDate.getYear)))
                case ColNames.PROC_MONTH => df.withColumn(ColNames.PROC_MONTH, lit("%02d".format(processingDate.getMonthValue)))
                case ColNames.PROC_DAY => df.withColumn(ColNames.PROC_DAY, lit("%02d".format(processingDate.getDayOfMonth)))
                // skip uknown ones
                case _ => df
            }
        )

        (partitionedResult, targetCols)
    }

    def getPartitionedWriter(result : DataFrame, arguments : TimeIntervalArguments) : DataFrameWriter[Row] = {
        // if partitionBy is used
        if(arguments.partitionBy != null) {
            // attempt creating them in case they are named like specific placeholders
            val (partitionedResult, targetCols) = partitionByStandardCols(result, arguments)
            val writer = partitionedResult.coalesce(Configs.NUM_TARGET_PARTITIONS).write.mode(SaveMode.Overwrite)

            // validate dataframe - check if all columns exist with that name
            if(!targetCols.filterNot(partitionedResult.columns.toSet).isEmpty){
                log.error("Partitioned dataframe does not contain all target columns")
                // todo: should we just sys.exit here?
                sys.exit(5)
                // or we fail silently and let write the result?
                // writer
            }else{
                // partition by target cols - they are all available already
                writer.partitionBy(targetCols : _*)
            }
        }else{
            result.coalesce(Configs.NUM_TARGET_PARTITIONS).write.mode(SaveMode.Overwrite)
        }
    }

    lazy val schemes = Array("http","https")
    lazy val urlValidator = new UrlValidator(schemes, null, UrlValidator.ALLOW_LOCAL_URLS)
}