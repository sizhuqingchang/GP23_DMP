package com.tag

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object Tagad extends Tag {
  override def makeTag(row: Any*): List[(String, Int)] = {
    //创建可变list,用于存数据
    var list=List[(String,Int)]()

    //获取数据类型
    val line=row(0).asInstanceOf[Row]

    //获取广告位类型和名称
    val adspacetype: Int = line.getAs[Int]("adspacetype")
    val adspacetypename:String= line.getAs[String]("adspacetypename")

    if(adspacetype>9){
      list:+=("LC"+adspacetype,1)
    }else if(adspacetype>0){
      list:+=("LC0"+adspacetype,1)
    }else{

    }

    if(StringUtils.isNotBlank(adspacetypename)){
      list:+=("LN"+adspacetypename,1)
    }

    list
  }
}
