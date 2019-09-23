package com.tag

import java.util.Calendar

import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SparkSession}


object TagContext {
  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("makeTag")
      .master("local[2]")
      .getOrCreate()

    //获取数据
    val dataFrame: DataFrame = sparkSession.read.parquet("D://gp23_DMP")

    val day=Calendar.getInstance().toString
    //调用HBase Api
    val load= ConfigFactory.load()

    //获取表名称
    val tableName: String = load.getString("HBase.tableName")

    //创建Hadoop任务
    val hadCon= sparkSession.sparkContext.hadoopConfiguration

    //配置Hbase连接
    hadCon.set("hbase.zookeeper.quorum",load.getString("HBASE.Host"))

    //获取连接
    val hbCon: Connection = ConnectionFactory.createConnection(hadCon)
    val hbAdmin: Admin = hbCon.getAdmin

    //判断当前表是否被使用
    if(!hbAdmin.tableExists(TableName.valueOf(tableName))){
      println("当前表可用")
      val nameNameDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      val hColumnDescriptor = new HColumnDescriptor("tags")

      nameNameDescriptor.addFamily(hColumnDescriptor)
      hbAdmin.createTable(nameNameDescriptor)
      hbAdmin.close()
      hbCon.close()
    }

    val jobConf = new JobConf(hadCon)
    //指定输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定输出到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)


    dataFrame.rdd.map(line=>{
      //获取用户id
      val userID: String = TagUtils.getUserid(line)

      //获取广告标签
      val adList: List[(String, Int)] = Tagad.makeTag(line)

      //获取App名称标签
      val appList: List[(String, Int)] = Tagapp.makeTag(line)

      //获取channel标签
      val chList: List[(String, Int)] = Tagchannel.makeTag(line)

      //获取设备操作系统标签
      val osList: List[(String, Int)] = TagOS.makeTag(line)

      //获取设备联网方式标签
      val netList: List[(String, Int)] = TagNetwork.makeTag(line)

      //获取典型运营商标签
      val ispList: List[(String, Int)] = TagISP.makeTag(line)

      //获取商圈标签
      val busiList: List[(String, Int)] = Tagbusi.makeTag(line)

      val totalList: List[(String, Int)] = adList++appList++chList++osList++netList++ispList++busiList
      (userID,totalList)
    }).reduceByKey((list1,list2)=>{
      (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
      .toList
    })
      .map{
        case(userId,userTags)=>{
          val put=new Put(Bytes.toBytes(userId))
          put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(day),Bytes.toBytes(userTags.mkString(",")))
          (new ImmutableBytesWritable(),put)
        }
      }
      .saveAsHadoopDataset(jobConf)

    sparkSession.stop()
  }
}
