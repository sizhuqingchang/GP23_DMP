package com.location

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ProvinceCityCt {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("CT")
      .master("local[2]")
      .getOrCreate()
    val dataSet: DataFrame = sparkSession.read.parquet("D://gp23_DMP")

    dataSet.createOrReplaceTempView("log")
    val res1=sparkSession.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
    // res1.write.partitionBy("provincename","cityname").json("D://gp23_output_dmp")


    val load: Config = ConfigFactory.load

    val properties = new Properties()

    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    res1.write.mode(SaveMode.Append)
      .jdbc(load.getString("jdbc.url"),load.getString("jdbc.table"),properties)

    sparkSession.stop()
  }
}
