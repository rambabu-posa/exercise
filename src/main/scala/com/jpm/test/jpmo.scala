package com.jpm.test

import org.apache.spark.sql._
import org.apache.spark.sql.types.{
  StructType, StructField, StringType}
import org.apache.spark.sql.Row
import util.jpm_test_utils._

object Jpmo extends App {

  val config: AppConfig = AppConfig()
  val countryNames = config.countryConfig.names
  val prefix = config.countryConfig.prefix
  val postfix = config.countryConfig.postfix
  val schemaConf = config.schemaConf
  val ignoreSymbols = config.ignoredSymbols
  val defaults = config.defaultsConfig
  val printOption = config.printOption
  val schema = StructType(
    StructType(StructField("country", StringType, true) :: Nil)
      ++ StructType(schemaConf.map(x => StructField(x(0), inferType(x(1)), true))))

  try {
  val spark = SparkSession.builder()
    .appName("JPMC")
    .master("local[1]")
    .getOrCreate()

  var df = spark.createDataFrame(spark.sqlContext.sparkContext.emptyRDD[Row], schema)

  val countryList = countryNames.map(_.trim).map(country => {
    val rows = scala.io.Source.fromURL(prefix + country + postfix)
      .mkString
      .split("\n")
      .toList.map(_.trim)
      .filter(filterInValidData(_,country))
      .map(country + " " + _)
      //.map(_.split("\\s+").toList)
      .map(validateData)
      .map { x => Row(x: _*) }
    val rdd = spark.sparkContext.makeRDD(rows)
    df = df.union(spark.sqlContext.createDataFrame(rdd, schema))

  })
  df.cache()
  println(s"countries : $countryNames")
  analytics.rankStationsByOnline(df, schemaConf, defaults).printData("rankStationsByOnline")
  analytics.rankStationsByOnlinePerMonth(df,schemaConf,defaults).printData("rankStationsByOnlinePerMonth")
  analytics.rankStationsByRainfall(df,defaults).printData("rankStationsByRainfall")
  analytics.rankStationsBySunshine(df,defaults).printData("rankStationsBySunshine")
  analytics.worstRainfall(df,defaults).printData("worstRainfall")
  analytics.bestSunshine(df,defaults).printData("bestSunshine")
  analytics.yearWiseMetrics(df.filter("year=5"),schemaConf,defaults).printData("yearWiseMetrics")

} catch {
  case ex:Exception=>
    println(s"message: ${ex.getMessage}")
    println(s"stack trace: ${ex.getStackTrace}")
}

}
