package com.github.pilillo

import java.time.format.DateTimeFormatter
import java.time.ZoneId

object Settings {

  object ColNames {
    val YEAR = "year"
    val MONTH = "month"
    val DAY = "day"

    val VERSION = "version"

    val DATE_FILTER_UTC = "date_filter_utc"

    // suggester
    val COLUMN_NAME = "column_name"
    val CONSTRAINT = "constraint"
    val DESCRIPTION = "description"
    val RULE = "rule"
    val RULE_EXTENDED_DESCRIPTION = "rule_extended_description"

    // profiler
    val COMPLETENESS = "completeness"
    val APPROX_NUM_DISTINCT_VALS = "approximate_number_distinct_values"
    val DATA_TYPE = "data_type"
    val IS_DATATYPE_INFERRED = "is_datatype_inferred"
    val TYPE_COUNTS = "type_counts"
    val HISTOGRAM = "histogram"
    val KLL = "kll"
    val MEAN = "mean"
    val MAXIMUM = "maximum"
    val MINIMUM = "minimum"
    val SUM = "sum"
    val STD_DEV = "std_dev"
    val APPROX_PERCENTILES = "approx_percentiles"

    // PROCESSING TIME
    val PROC_YEAR = s"PROC_YEAR"
    val PROC_MONTH = s"PROC_MONTH"
    val PROC_DAY = s"PROC_DAY"
    // INTERVAL START DATE
    val START_YEAR = s"START_YEAR"
    val START_MONTH = s"START_MONTH"
    val START_DAY = s"START_DAY"
    // INTERVAL END DATE
    val END_YEAR = s"END_YEAR"
    val END_MONTH = s"END_MONTH"
    val END_DAY = s"END_DAY"
  }

  object Formats {
    val dateDelimiter = "-"
    val inputDateFormat = s"yyyy${dateDelimiter}MM${dateDelimiter}dd"
    val inputDateFormatter = DateTimeFormatter.ofPattern(inputDateFormat)

    val zoneId = ZoneId.of("UTC") // or: ZoneId.of("Europe/Oslo");

    val PARTITIONBY_SPLIT_CHAR = "," // split by ,
  }

  object Configs {
    val TEST_TRAIN_RATIO = 0.1
    val NUM_TARGET_PARTITIONS = 1
  }
}
