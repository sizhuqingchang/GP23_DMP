package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object Tagarea extends Tag{
  override def makeTag(row:Any*):List[(String,Int)]= {

    //创建可变list集合
    var list = List[(String, Int)]()

    //获取数据类型
    val line = row(0).asInstanceOf[Row]

    val provincename: String = line.getAs[String]("provincename")
    if(StringUtils.isNotBlank(provincename)){
      list:+=("ZP"+provincename,1)
    }

    val cityname: String = line.getAs[String]("cityname")
    if(StringUtils.isNotBlank(cityname)){
      list:+=("ZP"+cityname,1)
    }

    list
  }
}
