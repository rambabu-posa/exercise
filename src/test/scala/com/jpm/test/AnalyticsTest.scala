package com.jpm.test
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.scalatest.{FlatSpec, Matchers}

class AnalyticsTest extends FlatSpec with Matchers with BaseSparkTest {

  // Input test dataframe
  val input: DataFrame = df("country" -> StringType,
    "year" -> IntegerType,
    "month" -> IntegerType,
    "tmin" -> DoubleType,
    "tmax" -> DoubleType,
    "af_days" -> IntegerType,
    "rain" -> DoubleType,
    "sunshine" -> DoubleType
  )(
    // Country     ,     Year, Month, Tmin, Tmax, af, rain, sun
    Seq("aberporthdata", 1941,   1,   4.0,   1.9,  1,  4.0, 4.0),
    Seq("aberporthdata", 1942,   5,   5.0,   2.0,  2,  5.0, 2.0),
    Seq("aberporthdata", 1943,   5,   6.0,   2.1,  3,  6.0, 3.0),
    Seq("leuchars",      1941,   1,   5.0,   1.9,  2,  2.0, 6.0),
    Seq("leuchars",      1942,   5,   4.0,   2.1,  2,  4.0, 4.0)
  )

  // schema same as in configuration
  val schemaConf: List[List[String]] = List(List("year","Int"),
    List("month","Int"),
    List("tmax","Double"),
    List("tmin","Double"),
    List("af_days","Int"),
    List("rain","Double"),
    List("sunshine","Double"))

  // default values
  val defaults:DefaultsConfig = DefaultsConfig("-1","-1000","")

  // problem 1 test
  "rankStationsByOnline " should " rank high metrics country" in {
    val result = Analytics.rankStationsByOnline(input,schemaConf,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq(1,"aberporthdata",15),
      Seq(2,"leuchars",10)
    )
    result.dfShouldEqual(expected)

  }

  // problem 1 test
  "rankStationsByOnlinePerMonth " should " rank high metrics country" in {

    val result = Analytics.rankStationsByOnlinePerMonth(input,schemaConf,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "metricsCount" -> IntegerType
    )(
      Seq(1,"aberporthdata",3),
      Seq(2,"leuchars",2)
    )
    result.dfShouldEqual(expected)

  }

  // problem 2 test
  "rankStationsByRainfall " should " rank high metrics rain countries" in {

    val result = Analytics.rankStationsByRainfall(input,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "avgRain" -> DoubleType
    )(
      Seq(1,"aberporthdata",5.0),
      Seq(2,"leuchars",3.0)
    )
    result.dfShouldEqual(expected)

  }

  // problem 2 test
  "rankStationsBySunshine " should " rank high metrics sun hours countries" in {

    val result = Analytics.rankStationsBySunshine(input,defaults)
    val expected =  df("rank" -> IntegerType,
      "country" -> StringType,
      "avgSunshine" -> DoubleType
    )(
      Seq(1,"leuchars",5.0),
      Seq(2,"aberporthdata",3.0)
    )
    result.dfShouldEqual(expected)

  }

  // problem 4 test
  "worstRainfall " should " provide high rain countries with dates" in {

    val result = Analytics.worstRainfall(input,defaults)
    val expected =  df("country" -> StringType,
      "year" -> IntegerType,
      "month" -> IntegerType,
      "max" -> DoubleType
    )(
      Seq("aberporthdata",1943,5,6.0),
      Seq("leuchars",1942,5,4.0)
    )
    result.dfShouldEqual(expected)

  }

  // problem 4 test
  "bestSunshine " should " provide high sun countries with dates" in {

    val result = Analytics.bestSunshine(input,defaults)
    val expected =  df("country" -> StringType,
      "year" -> IntegerType,
      "month" -> IntegerType,
      "max" -> DoubleType
    )(
      Seq("aberporthdata",1941,1,4.0),
      Seq("leuchars",1941,1,6.0)
    )
    result.dfShouldEqual(expected)

  }

  // problem 5 test
  "yearWiseMetrics " should " provide average of all fields in schema" in {

    val result = Analytics.aggMetrics(input,schemaConf,defaults)

    val expected =  df("country" -> StringType,
      "tminAvg" -> DoubleType,
      "tmaxAvg" -> DoubleType,
      "af_daysAvg" -> DoubleType,
      "rainAvg" -> DoubleType,
      "sunshineAvg" -> DoubleType,
      "tminMin" -> DoubleType,
      "tminMinYear" -> StringType,
      "tmaxMin" -> DoubleType,
      "tmaxMinYear" -> StringType,
      "af_daysMin" -> IntegerType,
      "af_daysMinYear" -> StringType,
      "rainMin" -> DoubleType,
      "rainMinYear" -> StringType,
      "sunshineMin" -> DoubleType,
      "sunshineMinYear" -> StringType,
      "tminMax" -> DoubleType,
      "tminMaxYear" -> StringType,
      "tmaxMax" -> DoubleType,
      "tmaxMaxYear" -> StringType,
      "af_daysMax" -> IntegerType,
      "af_daysMaxYear" -> StringType,
      "rainMax" -> DoubleType,
      "rainMaxYear" -> StringType,
      "sunshineMax" -> DoubleType,
      "sunshineMaxYear" -> StringType
    )(
      //                  Tmin, "year",  tmax, "year" , af,  "year", rain, "year", sun, "year"
      Seq("aberporthdata", 5.0,           2.0,         2.0,          5.0,         3.0,           // avg
                           4.0,  "1941",  1.9,  "1941", 1  , "1941",  4.0, "1941", 2.0, "1942",   // min
                           6.0,  "1943",  2.1,  "1943", 3  , "1943",  6.0, "1943", 4.0, "1941"),  // max

      //              Tmin, "year", tmax, "year", af,         year, rain, "year",  sun, "year",
      Seq("leuchars", 4.5,          2.0,          2.0,              3.0,           5.0,          // avg
                      4.0,  "1942", 1.9,  "1941", 2  , "1941 1942", 2.0,  "1941",  4.0, "1942",  // min
                      5.0,  "1941", 2.1,  "1942", 2  , "1941 1942", 4.0,  "1942",  6.0, "1941" ) // max
    )
    result.dfShouldEqual(expected)

  }

}