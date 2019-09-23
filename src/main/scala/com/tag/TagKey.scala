package com.tag

import com.utils.Tag
import org.apache.spark.sql.Row

object TagKey extends Tag{
  override def makeTag(row:Any*):List[(String,Int)]= {

    //创建可变list集合
    var list = List[(String, Int)]()

    //获取数据类型
    val line = row(0).asInstanceOf[Row]

    val keywords: String = line.getAs[String]("keywords")

    val arr: Array[String] = keywords.split("\\|",-1)
    for(str <- arr){
      if(str.length >=3 && str.length<=8)
        list:+=("K"+str,1)
    }

    list
  }

}
