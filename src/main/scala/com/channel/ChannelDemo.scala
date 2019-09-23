package com.channel

import com.utils.PCtUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object ChannelDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("media")
      .master("local[1]")
      .getOrCreate()
    val dataFrame: DataFrame = sparkSession.read.parquet("D://gp23_DMP")

    dataFrame.rdd.map(line=>{
      val channel: String =line.getAs[String]("channelid")
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

      (channel,fList)
    })
      .reduceByKey((list1,list2)=>{
      list1.zip(list2).map(x=>x._1+x._2)
    })
      .map(t=>t._1+","+t._2.mkString(","))
      .saveAsTextFile("D://gp23-output9")
    sparkSession.stop()
  }
}
