package com.media

import com.utils.PCtUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object MediaDemo {

  def main(args: Array[String]): Unit = {

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

    val dataFrame: DataFrame = sparkSession.read.parquet("D://gp23_DMP")


    //    使用spark-core 处理数据
        dataFrame.rdd.map(line=>{

          var appName = line.getAs[String]("appname")

          if(StringUtils.isBlank(appName)){
             appName = broCast.value
               .getOrElse(line.getAs[String]("appid"),"unknow")
          }
          val requestmode=line.getAs[Int]("requestmode")
          val processnode=line.getAs[Int]("processnode")
          val iseffective=line.getAs[Int]("iseffective")
          val isbilling=line.getAs[Int]("isbilling")
          val isbid=line.getAs[Int]("isbid")
          val iswin=line.getAs[Int]("iswin")
          val adorderid=line.getAs[Int]("adorderid")
          val winprice=line.getAs[Double]("winprice")
          val adpayment=line.getAs[Double]("adpayment")

          val prcList: List[Double] = PCtUtils.PrcList(requestmode,processnode)
          val cptList: List[Double] = PCtUtils.clickPt(requestmode,iseffective)
          val adptList: List[Double] = PCtUtils.adPt(iseffective,isbilling,isbid,iswin,adorderid,winprice,adpayment)
          val fList: List[Double] = prcList ++ cptList ++adptList
          (appName,fList)
        })
          .reduceByKey((list1,list2)=>{

            val tuples: List[(Double, Double)] = list1.zip(list2)
            tuples.map(x=>x._1+x._2)

          })
          .map(t=>t._1+","+t._2.mkString(","))
          .saveAsTextFile("D://gp23-output8")

    sparkSession.stop()
  }
}
