package com.jpm.test
import java.io.{File, FileOutputStream}

import com.jpm.test.Jpmo.schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, broadcast, desc, max, min, sum, row_number,col,concat_ws,collect_list}
object analytics {

  def printfunc(df:DataFrame) = {

    val fos = new FileOutputStream(new File("output/results2.txt"))
    Console.withOut(fos) {
     df.show
    }
  }

  def getDefault(colType:String,defaults:DefaultsConfig ) = colType match {
    case  "Int" => defaults.int
    case "Double" => defaults.float
    case _ => defaults.string
  }
  // helps to rank stations online by number of measures.
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

  // helps to rank stations online by number of measures by .
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


  // This is not used as implementations is changed
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

  // this returns max aggregated value

  // Solution for problem 4 with respect to high sunshine
  def aggColumn(df:DataFrame,colName:String,defaultVal:String,aggregator:String) = {
    import df.sqlContext.implicits._

    val ldf = df.filter(s"$colName!=$defaultVal")
    aggregator match {
      case "max" =>
        ldf.withColumn ("max", max (colName).over (Window.partitionBy ($"country") ) ).filter(s"$colName=max")
      case "min" =>
        ldf.withColumn ("min", min (colName).over (Window.partitionBy ($"country") ) ).filter(s"$colName=min")
      case _ =>
        ldf.withColumn ("avg", avg (colName).over (Window.partitionBy ($"country") ) )
    }
  }

  // Solution for problem 1 with respect to number of measures
  def rankStationsByOnline(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig ):Unit = {
    import df.sqlContext.implicits._

    df.map(x => checkOnline(x, schemaConf, defaults))
      .rdd.reduceByKey(_ + _)
      .toDF("country", "metricsCount")
      .withColumn("rank", row_number().over(Window.partitionBy().orderBy($"metricsCount".desc)))
      .select("rank","country","metricsCount")
      .show
  }

  // Solution for problem 1 with respect to measures presence per month
  def rankStationsByOnlinePerMonth(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig ):Unit = {
    import df.sqlContext.implicits._

    df.map(x => checkOnlinePerMonth(x, schemaConf, defaults))
      .rdd.reduceByKey(_ + _)
      .toDF("country","metricsCount")
      .withColumn("rank", row_number().over(Window.partitionBy().orderBy($"metricsCount".desc)))
      .select("rank","country","metricsCount")
      .show
  }

  // Solution for problem 2 with respect to  high rainfall
  def rankStationsByRainfall(df:DataFrame,defaults:DefaultsConfig):Unit = {
    import df.sqlContext.implicits._

    df.filter(s"rain!=${defaults.float}")
      .groupBy("country").agg(avg("rain").alias("avgRain"))
      .withColumn("rank", row_number().over(Window.partitionBy().orderBy($"avgRain".desc)))
      .select("rank","country","avgRain")
      .show
  }

  // Solution for problem 2 with respect to high sunshine
  def rankStationsBySunshine(df:DataFrame,defaults:DefaultsConfig):Unit = {
    aggColumn(df,"sunshine",defaults.float,"max").select("country","year","month","max").show
  }

  // Solution for problem 4 with respect to high sunshine
  def worstRainfall(df:DataFrame,defaults:DefaultsConfig):Unit = {
    aggColumn(df,"rain",defaults.float,"max").select("country","year","month","max").show
  }

  // Solution for problem 4 with respect to high sunshine
  def avgRainfall(df:DataFrame,defaults:DefaultsConfig):Unit = {
    aggColumn(df,"sunshine",defaults.float,"avg").select("country","year","month","avg").show
  }


  def bestSunshinefall(df:DataFrame,defaults:DefaultsConfig):Unit = {
    import df.sqlContext.implicits._

    df.filter(s"sunshine!=${defaults.float}")
      .withColumn("bestSunshine", max("sunshine").over(Window.partitionBy($"country")))
      .filter("sunshine=bestSunshine")
      .select("country","year","month","bestSunshine")
      .show
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


  def yearWiseMetrics(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig) = {
    var itrDf = df.select("country").distinct()
    List("avg","min","max").foreach( agg =>
    (2 until schemaConf.length).map(index => {

      val year = schemaConf(index)(0)+s"_${agg}_year"
      val aggr = schemaConf(index)(0)+s"_${agg}_value"

      val metricDf =
        (if(agg == "avg"){
          aggColumn(df, schemaConf(index)(0), getDefault(schemaConf(index)(1), defaults), agg).selectExpr("country cntry",s"$agg $aggr").distinct()
        }
        else {
          var tempDf = aggColumn(df, schemaConf(index)(0), getDefault(schemaConf(index)(1), defaults), agg).selectExpr("country cntry", s"cast(year as String) ${year}", s"$agg $aggr")
          (if (tempDf.count() > 1) tempDf.groupBy("cntry", s"$aggr").agg(concat_ws(" ", collect_list(year)) as year) else tempDf)
        })
      itrDf = itrDf.join(broadcast(metricDf),col("country") === col("cntry")).drop("cntry")

    }
    ))
    itrDf.show
  }
}
