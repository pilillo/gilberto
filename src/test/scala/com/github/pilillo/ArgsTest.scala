package com.github.pilillo

import com.amazon.deequ.repository.mastro.MastroSerde
import com.github.pilillo.commons.TimeInterval
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.commons.validator.routines.UrlValidator
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class ArgsTest extends FunSuite with DataFrameSuiteBase with Checkers {

  test("input arguments parsing"){
    val args1 = Array[String](
      "--action", "test",
      "--source", "b",
      "--destination", "c",
      "--from", "2020-01-01",
      "--to", "2020-01-01"
    )
    val arguments1 = TimeInterval.parse(args1)

    // required args
    assert("test", arguments1.get.action)
    assert("b", arguments1.get.source)
    assert("c", arguments1.get.destination)
    assert("2020-01-01", arguments1.get.dateFrom)
    assert("2020-01-01", arguments1.get.dateTo)

    // optional args
    assert(null, arguments1.get.repository)

    val args2 = args1 ++ Array[String]("--repository", "http://localhost", "--partition-by", "year,month,day")
    val arguments2 = TimeInterval.parse(args2)
    assert( "http://localhost", arguments2.get.repository)
    assert( "year,month,day", arguments2.get.partitionBy)
  }

  test("resultkey"){
    val args1 = Array[String](
      "--action", "test",
      "--source", "b",
      "--destination", "c",
      "--from", "2020-01-01",
      "--to", "2020-01-01",
      "--partition-by", "PROC_YEAR,PROC_MONTH,PROC_DAY"
    )
    val arguments = TimeInterval.parse(args1)
    val resultKey = Gilberto.getResultKey(arguments.get)

    assert("PROC_YEAR", resultKey.tags.toList(0)._1)
    assert("PROC_MONTH", resultKey.tags.toList(1)._1)
    assert("PROC_DAY", resultKey.tags.toList(2)._1)
  }

  test("url validator"){
    val schemes = Array("http","https")
    val urlValidator = new UrlValidator(schemes, null, UrlValidator.ALLOW_LOCAL_URLS)
    assert(
      urlValidator.isValid("ftp://foo.bar.com/") == false
    )
    assert(
      urlValidator.isValid("http://gigio.com")
    )
    assert(
      urlValidator.isValid("http://localhost")
    )
  }

  test("metricset serde"){

    val input =
      """
        |{
        |"name" : "gigio",
        |"version" : "1",
        |"description" : "test ms",
        |"labels":{"year":"2021"},
        |"metrics":
        |[
        |  {
        |    "resultKey": {
        |      "dataSetDate": 1630876393300,
        |      "tags": {}
        |    },
        |    "analyzerContext": {
        |      "metricMap": [
        |        {
        |          "analyzer": {
        |            "analyzerName": "Size"
        |          },
        |          "metric": {
        |            "metricName": "DoubleMetric",
        |            "entity": "Dataset",
        |            "instance": "*",
        |            "name": "Size",
        |            "value": 5.0
        |          }
        |        },
        |        {
        |          "analyzer": {
        |            "analyzerName": "Minimum",
        |            "column": "numViews"
        |          },
        |          "metric": {
        |            "metricName": "DoubleMetric",
        |            "entity": "Column",
        |            "instance": "numViews",
        |            "name": "Minimum",
        |            "value": 0.0
        |          }
        |        }
        |      ]
        |    }
        |  }
        |]
        |}
        |""".stripMargin


    val ms = MastroSerde.deserialize(input)

    assert("gigio", ms.name)
    assert("1", ms.version)

    val msString = MastroSerde.serialize(ms)
    assert(input.replaceAll("\\s", "") == msString.replaceAll("\\s", ""))
  }
}
