package com.jpm.test

import org.apache.spark.sql._
import org.apache.spark.sql.types.{
  StructType, StructField, StringType}
import org.apache.spark.sql.Row
import util.jpm_test_utils._

object Jpmo extends App {

   val spark = SparkSession.builder()
     .appName("JPMC")
     .master("local[1]")
     .getOrCreate()

  val config: AppConfig = AppConfig()
  val countryNames = config.countryConfig.names
  val prefix = config.countryConfig.prefix
  val postfix = config.countryConfig.postfix
  val schemaConf = config.schemaConf
  val ignoreSymbols = config.ignoredSymbols
  val defaults = config.defaultsConfig

  val schema = StructType(
    StructType(StructField("country", StringType, true):: Nil)
      ++ StructType(schemaConf.map(x =>StructField(x(0), inferType(x(1)), true))))

  var df = spark.createDataFrame(spark.sqlContext.sparkContext.emptyRDD[Row], schema)

  val countryList = countryNames.split(",").toList.map(_.trim).map( country => {
    val rows = scala.io.Source.fromURL(prefix + country + postfix)
      .mkString
      .split("\n")
      .toList.map(_.trim)
      .filter(filterInValidData)
      .map(country+" " + _)
      //.map(_.split("\\s+").toList)
      .map(validateData)
      .map{x => Row(x:_*)}
    val rdd = spark.sparkContext.makeRDD(rows)
    df = df.union(spark.sqlContext.createDataFrame(rdd, schema))

  })
  println(df.count)
  df.show(10)
  println(s"countries : $countryNames")
  //analytics.rankStationsByOnline(df,schemaConf,defaults)
  //analytics.rankStationsByRainfall(df,defaults)
  //analytics.rankStationsBySunshine(df,defaults)
  //analytics.worstRainfall(df)
  //analytics.bestSunshinefall(df)
  analytics.averagesAcrossMay(df,defaults)


  // val html = scala.io.Source.fromURL("https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/leucharsdata.txt").mkString
  // val list = html.split("\n").filter(_ != "")

}
