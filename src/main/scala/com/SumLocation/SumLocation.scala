package com.SumLocation


import com.utils.PCtUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SumLocation {
  def main(args: Array[String]): Unit = {

    //创建sparksession
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("CT")
      .master("local[2]")
      .getOrCreate()

    //获取数据
    val dataFrame: DataFrame = sparkSession.read.parquet("D://gp23_DMP")

    //使用 sparkSQL处理数据
//    val res1: DataFrame =dataFrame.select("provincename","cityname","requestmode","processnode","iseffective","isbilling",
//      "isbid","iswin","adorderid","winprice","adpayment")
//    res1.createOrReplaceTempView("sum_location")
//
//    val res2=sparkSession.sql("select provincename,cityname," +
//      "sum(case when requestmode = 1 AND processnode >= 1  then 1 else 0 END) originalRequest," +
//      "sum(case when requestmode = 1 AND processnode >=2  then 1 else 0 END) validRequest," +
//      "sum(case when requestmode = 1 AND processnode = 3  then 1 else 0 END) adRequest," +
//      "sum(case when iseffective = 1 AND isbilling = 1 and isbid = 1  then 1 else 0 END) bidding," +
//      "sum(case when iseffective = 1 AND isbilling = 1 and iswin = 1 and adorderid !=0 then 1 else 0 END)bidded,"+
//      "sum(case when requestmode = 2 AND iseffective = 1 then 1 else 0 END ) show," +
//      "sum(case when requestmode = 3 AND iseffective = 1   then 1 else 0 END) click," +
//      "sum(case when iseffective = 1 AND isbilling = 1 and iswin = 1  then winprice/1000 else 0 END) per," +
//      "sum(case when iseffective = 1 AND isbilling = 1 and iswin = 1  then adpayment/1000 else 0 END) cost " +
//      "from sum_location group by provincename,cityname")
//  res2.show()


    //使用spark-core 处理数据
    dataFrame.rdd.map(line=>{
      val provincename=line.getAs[String]("provincename")
      val cityname=line.getAs[String]("cityname")
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
      ((provincename,cityname),fList)
    })
      .reduceByKey((list1,list2)=>{

      val tuples: List[(Double, Double)] = list1.zip(list2)
      tuples.map(x=>x._1+x._2)

    })
      .map(t=>t._1._1+","+t._1._2+","+t._2.mkString(","))
      .saveAsTextFile("D://gp23-output3")

    sparkSession.stop()

  }

}
