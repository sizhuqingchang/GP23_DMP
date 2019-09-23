package com.tag

import com.utils.TagUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TagContext2 {

  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("makeTag")
      .master("local[2]")
      .getOrCreate()

    //获取数据
    val dataFrame: DataFrame = sparkSession.read.parquet("D://gp23_DMP")

    val allUser: RDD[(List[String], Row)] = dataFrame.rdd.map(row => {
      val allUserIdlist: List[String] = TagUtils.getAllUserId(row)
      (allUserIdlist, row)
    })


    //设置点
    val vector=allUser.flatMap(lines=>{

      val line=lines._2
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


      val lists=adList++appList++chList++ispList++netList++osList

      val VD: List[(String, Int)] = lines._1.map((_,0))++lists

      lines._1.map(str=>{
        if(lines._1.head.equals(str)){
          (str.hashCode.toLong,VD)
        }else{
          (str.hashCode.toLong,List.empty)
        }
      })
    })

    //设置边
    val edge=allUser.flatMap(lines=>{
      lines._1.map(uid=>
        Edge(lines._1.head.hashCode.toLong,uid.hashCode.toLong,0))
    })

    //构图
    val graph = Graph(vector,edge)

    //选举根点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    vertices.join(vector).map{
      case(uid,(cnId,tagsAndUserId))=>{
        (cnId,tagsAndUserId)
      }
    }.reduceByKey((list1,list2)=>{
      (list1++list2)
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum)
        .toList
    }).take(20).foreach(println)

    sparkSession.stop()
  }
}
