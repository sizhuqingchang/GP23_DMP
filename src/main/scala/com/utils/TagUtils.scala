package com.utils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagUtils {
  def getUserid(row:Row):String={
    row match{
      case t if StringUtils.isNotBlank(t.getAs[String]("imei")) =>t.getAs[String]("imei")
      case t if StringUtils.isNotBlank(t.getAs[String]("mac")) =>t.getAs[String]("mac")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfa")) =>t.getAs[String]("idfa")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudid")) =>t.getAs[String]("openudid")
      case t if StringUtils.isNotBlank(t.getAs[String]("androidid")) =>t.getAs[String]("androidid")

      case t if StringUtils.isNotBlank(t.getAs[String]("imeimd5")) =>t.getAs[String]("imeimd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("macmd5")) =>t.getAs[String]("macmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfamd5")) =>t.getAs[String]("idfamd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidmd5")) =>t.getAs[String]("openudidmd5")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididmd5")) =>t.getAs[String]("androididmd5")

      case t if StringUtils.isNotBlank(t.getAs[String]("imeisha1")) =>t.getAs[String]("imeisha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("macsha1")) =>t.getAs[String]("macsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("idfasha1")) =>t.getAs[String]("idfasha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("openudidsha1")) =>t.getAs[String]("openudidsha1")
      case t if StringUtils.isNotBlank(t.getAs[String]("androididsha1")) =>t.getAs[String]("androididsha1")

      case _ => "none"
    }
  }

  def getAllUserId(t:Row):List[String]={
    var list=List[String]()

    if (StringUtils.isNotBlank(t.getAs[String]("imei")))  list:+=t.getAs[String]("imei")
    if (StringUtils.isNotBlank(t.getAs[String]("mac")))  list:+=t.getAs[String]("mac")
    if (StringUtils.isNotBlank(t.getAs[String]("idfa")))  list:+=t.getAs[String]("idfa")
    if (StringUtils.isNotBlank(t.getAs[String]("openudid"))) list:+=t.getAs[String]("openudid")
    if (StringUtils.isNotBlank(t.getAs[String]("androidid"))) list:+=t.getAs[String]("androidid")

    if (StringUtils.isNotBlank(t.getAs[String]("imeimd5"))) list:+=t.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(t.getAs[String]("macmd5"))) list:+=t.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(t.getAs[String]("idfamd5"))) list:+=t.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(t.getAs[String]("openudidmd5"))) list:+=t.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(t.getAs[String]("androididmd5"))) list:+=t.getAs[String]("androididmd5")

    if (StringUtils.isNotBlank(t.getAs[String]("imeisha1"))) list:+=t.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(t.getAs[String]("macsha1")))  list:+=t.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(t.getAs[String]("idfasha1")))  list:+=t.getAs[String]("idfasha1")
    if (StringUtils.isNotBlank(t.getAs[String]("openudidsha1")))  list:+=t.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(t.getAs[String]("androididsha1")))  list:+=t.getAs[String]("androididsha1")

    list
  }
}
