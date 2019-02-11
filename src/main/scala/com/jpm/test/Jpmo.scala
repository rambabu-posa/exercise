package com.jpm.test

import com.jpm.test.util.JpmTestUtils._
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Jpmo extends App {

  // get Configs from config file
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
    .config("spark.sql.broadcastTimeout","3600")
    .getOrCreate()

  var df = spark.createDataFrame(spark.sqlContext.sparkContext.emptyRDD[Row], schema)

  // Creating dataframe with online data which includes country column
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

  // calling and printing analytic data
  Analytics.rankStationsByOnline(df, schemaConf, defaults).printData("rankStationsByOnline")
  Analytics.rankStationsByOnlinePerMonth(df,schemaConf,defaults).printData("rankStationsByOnlinePerMonth")
  Analytics.rankStationsByRainfall(df,defaults).printData("rankStationsByRainfall")
  Analytics.rankStationsBySunshine(df,defaults).printData("rankStationsBySunshine")
  Analytics.worstRainfall(df,defaults).printData("worstRainfall")
  Analytics.bestSunshine(df,defaults).printData("bestSunshine")
  Analytics.aggMetrics(df.filter("month=5"),schemaConf,defaults).printData("aggMetrics")
  spark.stop()

} catch {
  case ex:Exception=>
    println(s"message: ${ex.getMessage}")
    println(s"stack trace: ${ex.getStackTrace}")
}

}
