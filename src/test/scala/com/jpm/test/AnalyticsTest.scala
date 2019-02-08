package com.jpm.test
import org.apache.spark.sql.DataFrame
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
    Seq("aberporthdata",1941,1,4,1.9,1,4.0,3.0),
    Seq("aberporthdata",1942,5,5,2.0,2,5.0,2.0),
    Seq("aberporthdata",1943,5,6,2.1,3,6.0,4.0),
    Seq("leuchars",1941,1,4,1.9,2,2.0,6.0),
    Seq("leuchars",1942,5,5,2.1,2,4.0,4.0)
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

  "rankStationsByOnline " should " rank high metrics country" in {

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

  "rankStationsByOnlinePerMonth " should " rank high metrics country" in {

    val result = analyticsObject.rankStationsByOnlinePerMonth(input,schemaConf,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq(1,"aberporthdata",3),
      Seq(2,"leuchars",2)
    )
    result.dfShouldEqual(expected)

  }


  "rankStationsByRainfall " should " rank high metrics rain countries" in {

    val result = analyticsObject.rankStationsByRainfall(input,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq(1,"aberporthdata",5),
      Seq(2,"leuchars",3)
    )
    result.dfShouldEqual(expected)

  }

  "rankStationsBySunshine " should " rank high metrics sun hours countries" in {

    val result = analyticsObject.rankStationsBySunshine(input,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq(1,"leuchars",5),
      Seq(2,"aberporthdata",4)
    )
    result.dfShouldEqual(expected)

  }

  "worstRainfall " should " provide high rain countries with dates" in {

    val result = analyticsObject.worstRainfall(input,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq("aberporthdata",1943,5,6.0),
      Seq("leuchars",1942,5,4.0)
    )
    result.dfShouldEqual(expected)

  }

  "bestSunshine " should " provide high sun countries with dates" in {

    val result = analyticsObject.bestSunshine(input,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq("aberporthdata",1943,5,4.0),
      Seq("leuchars",1941,1,6.0)
    )
    result.dfShouldEqual(expected)

  }

}