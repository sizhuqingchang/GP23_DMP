package com.tag

import com.utils.Tag
import org.apache.spark.sql.Row

object TagNetwork extends Tag{
  override def makeTag(row:Any*):List[(String,Int)]= {

    //创建可变list集合
    var list = List[(String, Int)]()

    //获取数据类型
    val line = row(0).asInstanceOf[Row]

    val netname :String = line.getAs[String]("networkmannername")

    netname match{
      case "WIFI" =>list:+=("D00020001",1)
      case "4G" =>list:+=("D00020002",1)
      case "3G" =>list:+=("D00020003",1)
      case "2G" =>list:+=("D00020004",1)
      case _ =>list:+=("D00020005",1)
    }

    list
  }
}
