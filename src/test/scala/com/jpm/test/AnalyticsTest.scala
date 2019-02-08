package com.jpm.test
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class AnalyticsTest extends FlatSpec with Matchers with BaseSparkTest {

  val input: DataFrame = df("country" -> StringType,
    "year" -> IntegerType,
    "month" -> IntegerType,
    "tmin" -> DoubleType,
    "tmax" -> DoubleType,
    "af_days" -> IntegerType,
    "rain" -> DoubleType,
    "sunshine" -> DoubleType
  )(
    Seq("aberporthdata",1941,1,5.8,2.1,1,114.0,58.0),
    Seq("aberporthdata",1942,5,5.8,2.1,1,114.0,58.0),
    Seq("aberporthdata",1943,5,5.8,2.1,1,114.0,58.0),
    Seq("leuchars",1941,1,5.8,2.1,1,114.0,58.0),
    Seq("leuchars",1942,5,5.8,2.1,1,114.0,58.0)
  )

  val schemaConf: List[List[String]] = List(List("year","Int"),
    List("month","Int"),
    List("tmax","Double"),
    List("tmin","Double"),
    List("af_days","Int"),
    List("rain","Double"),
    List("sunshine","Double"))

  val analyticsObject = Analytics
  val defaults:DefaultsConfig = DefaultsConfig("-1","-1000","")

  "rankStationsByOnline " should " return high metrics country" in {

    val result = analyticsObject.rankStationsByOnline(input,schemaConf,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq(1,"aberporthdata",15),
      Seq(2,"leuchars",10)
    )
    result.dfShouldEqual(expected)

  }

}