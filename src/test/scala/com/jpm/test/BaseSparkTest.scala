package com.jpm.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Matchers, Suite}

trait BaseSparkTest extends BeforeAndAfterAll with Matchers {self: Suite =>
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  private var _spark = SparkSession.builder().appName("test").master("local[1]").config("spark.sql.shuffle.partitions", "2").getOrCreate()
  implicit def getSpark: SparkSession = _spark

  override def beforeAll(): Unit = {
    if(_spark == null) {
      _spark = SparkSession.builder().appName("test").master("local[1]").enableHiveSupport().getOrCreate()
    }
  }

//  override def afterAll(): Unit = {
//    if(_spark != null) {
//      _spark.stop()
//      _spark = null
//    }
//  }

  protected def df(spec: (String, DataType)*)(rows: Seq[Any]*): DataFrame = {
    val rowRdd: RDD[Row] = getSpark.sparkContext.parallelize(rows).map(Row.fromSeq)
    val struct = StructType(
      spec.map{case (name, dt) => StructField(name, dt)}
    )
    getSpark.createDataFrame(rowRdd, struct)
  }

  implicit class TestDF(df: DataFrame) {
    def dfShouldEqual(other: DataFrame): Unit = {
      df.collect() should contain theSameElementsAs other.collect()
    }
  }
}