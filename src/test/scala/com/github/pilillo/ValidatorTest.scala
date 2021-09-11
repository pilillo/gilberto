package com.github.pilillo

import com.amazon.deequ.checks.{Check, CheckLevel}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.repository.mastro.MastroMetricsRepository
import com.github.pilillo.pipelines.BatchValidator._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import com.amazon.deequ.repository.memory.InMemoryMetricsRepository
import com.amazon.deequ.repository.querable.QuerableMetricsRepository
import com.github.pilillo.commons.TimeInterval

import java.io.File
import scala.reflect.io.Directory

class ValidatorTest extends FunSuite with DataFrameSuiteBase with Checkers {

  test("parse code config"){

    /* Need to import the deequ libs to avoid:
    not found: value Check
      scala.tools.reflect.ToolBoxError: reflective compilation has failed:
    */
    val codeConfig = """
           import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
           Seq(
            Check(CheckLevel.Error, "data testing with error level")
              .hasSize(_ >0)
              .hasMin("numViews", _ > 12)
           )
         """
    val checks = getChecks(codeConfig)
    // 1 check with 2 constraints is defined - the check level is error
    assert(1, checks.length)
    assert(CheckLevel.Error, checks(0).level)
  }

  test("validator"){
    import spark.implicits._

    val data = spark.sparkContext.parallelize(
      Seq(
        (1, "Thingy A", "awesome thing.", "high", 0),
        (2, "Thingy B", "available at http://thingb.com", null, 0),
        (3, null, null, "low", 5),
        (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        (5, "Thingy E", null, "high", 12))
      )
      .toDF("id", "productName", "description", "priority", "numViews")

    // CORRECT CHECK
    var codeConfig = """
           import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
           Seq(
            Check(CheckLevel.Error, "data testing with error level")
              .hasSize(_ >0)
              .hasMin("numViews", _ <= 12)
           )
         """

    var result = data.validate(
      codeConfig,
      //"http://localhost",
      new InMemoryMetricsRepository()
    )
    assert(0, result)

    // FAILING CHECK
    codeConfig = """
           import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
           Seq(
            Check(CheckLevel.Error, "unit testing my data")
              .hasSize(_ >0)
              .hasMin("numViews", _ > 12)
           )
         """
    result = data.validate(
      codeConfig,
      new InMemoryMetricsRepository()
      //new FileSystemMetricsRepository(spark, "myrepo")
    )
    assert(4, result)

  }

  test("querable repo"){
    import spark.implicits._
    val data = spark.sparkContext.parallelize(
      Seq(
        (1, "Thingy A", "awesome thing.", "high", 0),
        (2, "Thingy B", "available at http://thingb.com", null, 0),
        (3, null, null, "low", 5),
        (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        (5, "Thingy E", null, "high", 12))
    ).toDF("id", "productName", "description", "priority", "numViews")

    val codeConfig = """
           import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
           Seq(
            Check(CheckLevel.Error, "unit testing my data")
              .hasSize(_ >0)
              .hasMin("numViews", _ > 12)
           )
         """
    val repoPath = "test-repo.parquet"
    val repo = QuerableMetricsRepository(spark, repoPath, 1)
    data.validate(codeConfig, repo)
    // by default, this creates a file of kind
    // {"entity":"Column","instance":"numViews","name":"Minimum","value":0,"dataset_date":1631313748911}
    // https://nrinaudo.github.io/scala-best-practices/unsafe/array_comparison.html
    assert(
      spark.read.parquet(repoPath).columns.sorted
        sameElements
        Array("dataset_date", "entity", "instance", "name", "value").sorted
    )
    new Directory(new File("test-repo.parquet")).deleteRecursively()

    // let's now use partition by to see if we got those additional columns from the tags
    val args = Array[String]("--action", "test",
      "--source", "b", "--destination", "c",
      "--from", "01/01/2020", "--to", "01/01/2020",
      "--partition-by", "PROC_YEAR,PROC_MONTH,PROC_DAY")
    val arguments = TimeInterval.parse(args)
    val resultKey = Gilberto.getResultKey(arguments.get)

    data.validate(codeConfig, repo, resultKey)
    assert(
      spark.read.parquet(repoPath).columns.sorted
        sameElements
        Array("PROC_YEAR", "PROC_MONTH", "PROC_DAY", "dataset_date", "entity", "instance", "name", "value").sorted
    )
    new Directory(new File("test-repo.parquet")).deleteRecursively()
  }

  //new MastroMetricsRepository(spark, "http://localhost")
}
