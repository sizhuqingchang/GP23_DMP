package com.tag

import com.utils.Tag
import org.apache.spark.sql.Row

object Tagchannel extends Tag{
  override def makeTag(row: Any*): List[(String, Int)] = {

    //创建可变list集合
    var list=List[(String,Int)]()

    //获取数据类型
    val line=row(0).asInstanceOf[Row]

    val channelId: Int = line.getAs[Int]("adplatformproviderid")

    if(channelId>=100000){
      list:+=("CN"+channelId,1)
    }

    list
  }

}
