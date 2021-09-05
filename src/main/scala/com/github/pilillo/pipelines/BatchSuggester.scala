package com.github.pilillo.pipelines

import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.github.pilillo.Settings.{ColNames, Configs}
import com.github.pilillo.commons.TimeIntervalArguments
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}

object BatchSuggester {
  val log : Logger = Logger.getLogger(getClass.getName)
  implicit class Suggester(df: DataFrame) {
    def suggest()
    //           (implicit spark : SparkSession)
    : DataFrame = {
        val spark = df.sparkSession
        import spark.implicits._
          ConstraintSuggestionRunner()
            .onData(df)
            .useTrainTestSplitWithTestsetRatio(Configs.TEST_TRAIN_RATIO)
            .addConstraintRules(Rules.DEFAULT)
            .run()
            .constraintSuggestions
            .mapValues(
              ss => ss.map(
                s => (s.columnName, s.constraint.toString, s.description, s.currentValue, s.suggestingRule.ruleDescription)
              )
            )
            .values
            .flatten
            .toSeq
            .toDF(
              ColNames.COLUMN_NAME,
              ColNames.CONSTRAINT,
              ColNames.DESCRIPTION,
              ColNames.RULE,
              ColNames.RULE_EXTENDED_DESCRIPTION
            )


    }
  }
}