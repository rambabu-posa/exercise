package com.jpm.test

import org.apache.spark.sql._
import org.apache.spark.sql.types.{
  StructType, StructField, StringType, IntegerType, DoubleType}
import org.apache.spark.sql.Row

object Jpmo extends App {

  def inferType(field: String) = field match {
    case "Int" => IntegerType
    case "Double" => DoubleType
    case "String" => StringType
    case _ => StringType
  }

  def getInt(data : String,ignoreSymbolsMap:Map[String,String] ): Int ={
    val intData = ignoreSymbolsMap.foldLeft(data)((a, b) => a.replaceAllLiterally(b._1, b._2)).trim
    if(intData.trim matches """\d+""") intData.toInt else defaults.int.toInt
  }

  def getDouble(data : String,ignoreSymbolsMap:Map[String,String] ): Double ={
    val intData = ignoreSymbolsMap.foldLeft(data)((a, b) => a.replaceAllLiterally(b._1, b._2)).trim
    if(intData.trim matches """[+-]?([0-9]*[.])?[0-9]+""") intData.toDouble else defaults.float.toDouble
  }

  def validateData(line:String) : List[Any] = {
    val rowData = line.split("\\s+")
    var returnData = List[Any](rowData(0))
    var columnNumber = 1
    val ignoreSymbolsMap = Map(ignoreSymbols map((_,"")):_*)
    schemaConf.map(x=> {
      x(1) match {
        case "Int" => returnData = returnData:+getInt(rowData(columnNumber),ignoreSymbolsMap)
          columnNumber+=1

        case "Double" => returnData = returnData:+getDouble(rowData(columnNumber),ignoreSymbolsMap)
          columnNumber+=1

        case _ => returnData = returnData:+getInt(rowData(columnNumber),ignoreSymbolsMap)
          columnNumber+=1
      }
    })
    returnData
  }

  def filterInValidData(line:String) : Boolean = {
     if(line == "" ) return false
     line.split("\\s")(0) matches """\d+"""
  }


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
  analytics.worstRainfall(df)


  // val html = scala.io.Source.fromURL("https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/leucharsdata.txt").mkString
  // val list = html.split("\n").filter(_ != "")

}
