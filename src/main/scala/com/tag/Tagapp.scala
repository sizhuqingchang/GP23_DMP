package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Tagapp extends Tag{
  override def makeTag(row:Any*):List[(String,Int)]={

    //创建可变list集合
    var list=List[(String,Int)]()

    //获取数据类型
    val line=row(0).asInstanceOf[Row]


    val sparkSession: SparkSession = SparkSession.builder()
      .appName("media")
      .master("local[1]")
      .getOrCreate()
    val files: RDD[String] = sparkSession.sparkContext.textFile("E://Bigedata/spark/项目day01/Spark用户画像分析/app_dict.txt")

    val map: collection.Map[String, String] = files.map(line => line.split("\\s", -1))
      .filter(x => x.length >= 5)
      .map(arr => (arr(4), arr(1)))
      .collectAsMap()

    val broCast: Broadcast[collection.Map[String, String]] = sparkSession.sparkContext.broadcast(map)

    var appname: String = line.getAs[String]("appname")

    if(StringUtils.isBlank(appname)){
      appname = broCast.value
        .getOrElse(line.getAs[String]("appid"),"unknow")
    }
    list:+=("APP"+appname,1)

    sparkSession.stop()
    list
  }

}
