package com.jpm.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{desc,sum,max,avg,broadcast}
object analytics {
  private def checkOnline(row:Row,schemaConf: List[List[String]],defaults:DefaultsConfig ) ={
    var metricsCount = 0
    (2 until schemaConf.length).map(index => {
      schemaConf(index)(1) match {
        case "Int" => if(row.getInt(index+1) != defaults.int) metricsCount+=1
        case "Double" => if(row.getDouble(index+1) != defaults.float) metricsCount+=1
        case _ => if(row.getString(index+1) != defaults.string) metricsCount+=1
      }
    })
    row.getString(0)->metricsCount
  }
  private def checkOnlinePerMonth(row:Row,schemaConf: List[List[String]],defaults:DefaultsConfig ) ={
    var metricsCount = 0
    (2 until schemaConf.length).map(index => {
      schemaConf(index)(1) match {
        case "Int" => if(row.getInt(index+1) != defaults.int) metricsCount=1
        case "Double" => if(row.getDouble(index+1) != defaults.float) metricsCount=1
        case _ => if(row.getString(index+1) != defaults.string) metricsCount=1
      }
    })
    row.getString(0)->metricsCount
  }
  //private def checkRainfall(row:Row,schemaConf: List[List[String]]) ={
  //  var metricsCount = 0
  //  val rainfallInder= for (i <- (0 until schemaConf.length) if(schemaConf(i)(0) == "rain")) i
  //
  //  (2 until schemaConf.length).map(index => {
  //    schemaConf(index)(1) match {
  //    }
  //  })
  //  row.getString(0)->metricsCount
  //}

  def rankStationsByOnline(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig ):Unit = {
    import df.sqlContext.implicits._
    df.map(x => checkOnline(x, schemaConf, defaults)).rdd.reduceByKey(_ + _).toDF("Country","MetricsCount").sort(desc("MetricsCount")).show()
  }

  def rankStationsByOnlinePerMonth(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig ):Unit = {
    import df.sqlContext.implicits._
    df.map(x => checkOnline(x, schemaConf, defaults)).rdd.reduceByKey(_ + _).toDF("Country","MetricsCount").sort(desc("MetricsCount")).show()
  }

  def rankStationsByRainfall(df:DataFrame,defaults:DefaultsConfig):Unit = {
    df.filter(s"rain!=${defaults.float}").groupBy("country").agg(sum("rain").alias("rainfall")).sort(desc("rainfall")).show()
  }

  def rankStationsBySunshine(df:DataFrame,defaults:DefaultsConfig):Unit = {
    df.filter(s"sunshine!=${defaults.float}").groupBy("country").agg(sum("sunshine").alias("sunshine")).sort(desc("sunshine")).show()
  }

  def worstRainfall(df:DataFrame):Unit = {
    val broadcastDf = broadcast(df.groupBy("country").agg(max("rain").alias("maxRain")).toDF("countryName","maxRain"))
    df.join(broadcast(broadcastDf),df("country")===broadcastDf("country") && df("rain") === broadcastDf("maxRain")).show()
  }

  def bestSunshinefall(df:DataFrame):Unit = {
    val broadcastDf = broadcast(df.groupBy("country").agg(max("sunshine").alias("maxSunshine")).toDF("countryName","maxRain"))
    df.join(broadcastDf,df("country")===broadcastDf("country") && df("sunshine") === broadcastDf("maxSunshine")).show()
  }

  def averagesAcrossMay(df:DataFrame,defaults:DefaultsConfig):Unit = {
    df.filter("month = 5" ).filter(s"sunshine!=${defaults.float}").groupBy("country").agg(avg("sunshine").alias("avgSunshine")).show
    df.filter("month = 5" ).filter(s"rain!=${defaults.float}").groupBy("country").agg(avg("rain").alias("avgRain"),avg("sunshine").alias("avgSunshine")).show
  }

  def bestRainfall(df:DataFrame):Unit = {
    val groupedDf = df.groupBy("country").agg(max("rain").alias("maxRain"))
    df.join(groupedDf,df("country")===groupedDf("country") && df("rain") === groupedDf("maxRain")).show()
  }
}
