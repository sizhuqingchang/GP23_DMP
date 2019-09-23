package com.tag

import com.utils.Tag
import org.apache.spark.sql.Row

object TagOS extends Tag{
  override def makeTag(row:Any*):List[(String,Int)]= {

    //创建可变list集合
    var list = List[(String, Int)]()

    //获取数据类型
    val line = row(0).asInstanceOf[Row]

    val client: Int = line.getAs[Int]("client")

    client match{
      case 1 =>list:+=("D00010001",1)
      case 2 =>list:+=("D00010002",1)
      case 3 =>list:+=("D00010003",1)
      case _ =>list:+=("D00010004",1)
    }

    list
  }
}
