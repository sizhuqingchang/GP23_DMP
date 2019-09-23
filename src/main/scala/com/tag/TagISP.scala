package com.tag

import com.utils.Tag
import org.apache.spark.sql.Row

object TagISP extends Tag{
  override def makeTag(row:Any*):List[(String,Int)]= {

    //创建可变list集合
    var list = List[(String, Int)]()

    //获取数据类型
    val line = row(0).asInstanceOf[Row]

    val ispname: String = line.getAs[String]("ispname")

    ispname match{
      case "移动" =>list:+=("D00030001",1)
      case "联通" =>list:+=("D00030002",1)
      case "电信" =>list:+=("D00030003",1)
      case _ =>list:+=("D00010004",1)
    }

    list
  }
}
