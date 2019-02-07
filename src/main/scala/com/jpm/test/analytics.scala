package com.jpm.test
import java.io.{File, FileOutputStream}

import com.jpm.test.Jpmo.schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, broadcast, desc, max, min, sum}
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
  private def cleanDefaults(row:Row,schemaConf: List[List[String]],defaults:DefaultsConfig ) ={
    var returnData = List[Any](row.getString(0))
    (0 until schemaConf.length).map(index => {
      schemaConf(index)(1) match {
        case "Int" => returnData = returnData:+ (if(row.getInt(index+1) != defaults.int.toInt) row.getInt(index+1) else 0)
        case "Double" => returnData = returnData:+ (if(row.getDouble(index+1) != defaults.float.toDouble) row.getDouble(index+1) else 0.0)
        case _ => returnData = returnData:+ (if(row.getString(index+1) != defaults.string) row.getString(index+1) else "")
      }
    })
    println(returnData)
    returnData
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
      df.map(x => checkOnline(x, schemaConf, defaults)).rdd.reduceByKey(_ + _).toDF("Country", "MetricsCount").sort(desc("MetricsCount")).coalesce(1).rdd.saveAsTextFile("output/res1")
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
    val fos = new FileOutputStream(new File("output/results2.txt"))
    Console.withOut(fos) {
      val broadcastDf = broadcast(df.groupBy("country").agg(max("rain").alias("maxRain")).toDF("countryName", "maxRain"))
      df.join(broadcast(broadcastDf), df("country") === broadcastDf("countryName") && df("rain") === broadcastDf("maxRain")).select("country", "year", "month", "maxRain").show()

    }
  }

  def bestSunshinefall(df:DataFrame):Unit = {
    val fos = new FileOutputStream(new File("results3.txt"))
    Console.withOut(fos) {
      val broadcastDf = broadcast(df.groupBy("country").agg(max("sunshine").alias("maxSunshine")).toDF("countryName", "maxSunshine"))
      df.join(broadcastDf, df("country") === broadcastDf("countryName") && df("sunshine") === broadcastDf("maxSunshine")).select("country", "year", "month", "maxSunshine").show()

    }
  }

  def averagesAcrossSun(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig):Unit = {
    import df.sqlContext.implicits._
    val fos = new FileOutputStream(new File("results7.txt"))
    Console.withOut(fos) {
      df.filter(s"sunshine!=${defaults.float}").groupBy("country").agg(avg("sunshine").alias("avgsunshine")).show
      //df.sqlContext.createDataFrame(df.filter("month = 4").rdd.map(cleanDefaults(_, schemaConf, defaults)).map(Row(_:_*)),schema).groupBy("country").agg(avg("tmax").alias("avgtmax"), avg("tmin").alias("avgtmin"), avg("af_days").alias("avgaf_days"), avg("rain").alias("avgRain"), avg("sunshine").alias("avgSunshine")).show
    }
  }

  def averagesAcrossMay(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig):Unit = {
    import df.sqlContext.implicits._
    val fos = new FileOutputStream(new File("output/results5.txt"))
    Console.withOut(fos) {
      df.sqlContext.createDataFrame(df.filter("month = 4").rdd.map(cleanDefaults(_, schemaConf, defaults)).map(Row(_:_*)),schema).groupBy("country").agg(avg("tmax").alias("avgtmax"), avg("tmin").alias("avgtmin"), avg("af_days").alias("avgaf_days"), avg("rain").alias("avgRain"), avg("sunshine").alias("avgSunshine")).show
    }
  }

  def bestRainfall(df:DataFrame):Unit = {
    val groupedDf = df.groupBy("country").agg(max("rain").alias("maxRain"))
    df.join(groupedDf,df("country")===groupedDf("country") && df("rain") === groupedDf("maxRain")).show()
  }


  def bestSunshinefallWindow(df:DataFrame):Unit = {
    val w = Window.partitionBy("country")
    val fos = new FileOutputStream(new File("results4.txt"))
    Console.withOut(fos) {
      val df2 = df.withColumn("maxSunshine", max("sunshine").over(w))
        .filter("maxSunshine=sunshine")
      df2.show()
    }
  }

  def avgWindow(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig):Unit = {
    val w = Window.partitionBy("country")
    import df.sqlContext.implicits._
    val fos = new FileOutputStream(new File("results4.txt"))
    Console.withOut(fos) {
      val df2 = df.sqlContext.createDataFrame(df.filter("month = 4").rdd.map(cleanDefaults(_, schemaConf, defaults)).map(Row(_:_*)),schema).withColumn("maxSunshine", max("sunshine").over(w))
        .withColumn("minSunshine", min("sunshine").over(w)).filter("maxSunshine=sunshine or minSunshine=sunshine")
      df2.show()
    }
  }

}
