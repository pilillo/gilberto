package com.github.pilillo

import com.github.pilillo.commons.TimeInterval
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.commons.validator.routines.{DomainValidator, UrlValidator}
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

class ArgsTest extends FunSuite with DataFrameSuiteBase with Checkers {
  test("input arguments parsing"){
    val args1 = Array[String](
      "--action", "test",
      "--source", "b",
      "--destination", "c",
      "--from", "01/01/2020",
      "--to", "01/01/2020"
    )
    val arguments1 = TimeInterval.parse(args1)

    // required args
    assert("test", arguments1.get.action)
    assert("b", arguments1.get.source)
    assert("c", arguments1.get.destination)
    assert("01/01/2020", arguments1.get.dateFrom)
    assert("01/01/2020", arguments1.get.dateTo)

    // optional args
    assert(null, arguments1.get.repository)

    val args2 = args1 ++ Array[String]("--repository", "http://localhost", "--partition-by", "year,month,day")
    val arguments2 = TimeInterval.parse(args2)
    assert( "http://localhost", arguments2.get.repository)
    assert( "year,month,day", arguments2.get.partitionBy)
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
}
