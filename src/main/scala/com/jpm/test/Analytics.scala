package com.jpm.test

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.jpm.test.util.JpmTestUtils.getDefault

object Analytics {


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

  // returns aggregated values
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
  def rankStationsByOnline(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig ) = {
    import df.sqlContext.implicits._

    df.map(x => checkOnline(x, schemaConf, defaults))
      .rdd.reduceByKey(_ + _)
      .toDF("country", "metricsCount")
      .withColumn("rank", row_number().over(Window.partitionBy().orderBy($"metricsCount".desc)))
      .select("rank","country","metricsCount")
  }

  // Solution for problem 1 with respect to measures presence per month
  def rankStationsByOnlinePerMonth(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig ) = {
    import df.sqlContext.implicits._

    df.map(x => checkOnlinePerMonth(x, schemaConf, defaults))
      .rdd.reduceByKey(_ + _)
      .toDF("country","metricsCount")
      .withColumn("rank", row_number().over(Window.partitionBy().orderBy($"metricsCount".desc)))
      .select("rank","country","metricsCount")
  }

  // Solution for problem 2 with respect to  high rainfall
  def rankStationsByRainfall(df:DataFrame,defaults:DefaultsConfig) = {
    import df.sqlContext.implicits._

    df.filter(s"rain!=${defaults.float}")
      .groupBy("country").agg(avg("rain").alias("avgRain"))
      .withColumn("rank", row_number().over(Window.partitionBy().orderBy($"avgRain".desc)))
      .select("rank","country","avgRain")
  }

  // Solution for problem 2 with respect to high sunshine
  def rankStationsBySunshine(df:DataFrame,defaults:DefaultsConfig) = {
    import df.sqlContext.implicits._

    df.filter(s"sunshine!=${defaults.float}")
      .groupBy("country").agg(avg("sunshine").alias("avgSunshine"))
      .withColumn("rank", row_number().over(Window.partitionBy().orderBy($"avgSunshine".desc)))
      .select("rank","country","avgSunshine")
  }

  // Solution for problem 4 with respect to high rain
  def worstRainfall(df:DataFrame,defaults:DefaultsConfig) = {
    aggColumn(df,"rain",defaults.float,"max").select("country","year","month","max")
  }

  // Solution for problem 4 with respect to high sun hours
  def bestSunshine(df:DataFrame,defaults:DefaultsConfig) = {
    aggColumn(df,"sunshine",defaults.float,"max").select("country","year","month","max")
  }

  // Solution for problem 5 collects all may metrics
  def aggMetrics(df:DataFrame,schemaConf: List[List[String]],defaults:DefaultsConfig) = {
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
    itrDf.select(  "country", "tmin_avg_value", "tmax_avg_value", "af_days_avg_value", "rain_avg_value", "sunshine_avg_value", "tmin_min_value", "tmin_min_year", "tmax_min_value", "tmax_min_year", "af_days_min_value", "af_days_min_year", "rain_min_value", "rain_min_year", "sunshine_min_value", "sunshine_min_year", "tmin_max_value", "tmin_max_year", "tmax_max_value", "tmax_max_year", "af_days_max_value", "af_days_max_year", "rain_max_value", "rain_max_year", "sunshine_max_value", "sunshine_max_year").toDF()
  }
}
