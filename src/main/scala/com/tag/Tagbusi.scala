package com.tag

import com.utils.{AmapUtil, JedisConnectPool, String2Type, Tag}
import org.apache.spark.sql.Row
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils

object Tagbusi extends Tag{
  override def makeTag(row: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()

    val line: Row = row(0).asInstanceOf[Row]
    val long: Double =String2Type.toDouble(line.getAs[String]("long"))
    val lat: Double = String2Type.toDouble(line.getAs[String]("lat"))

    if(long>=72 &&long<=156 && lat>=3 && lat<=53)
    {

      val business: String = getBusiness(long,lat)

      if(StringUtils.isNotBlank(business)){
        val arr: Array[String] = business.split(",")
        arr.foreach(arr=>{
          list:+=(arr,1)
        })
      }
    }

    list
  }

  def getBusiness(long:Double,lat:Double):String={
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,6)

   var business= redis_queryBusiness(geoHash)

    if(business==null){

      val business: String = AmapUtil.getBusiFromAmap(long,lat)

      if(business !=null &&business.length>0){
        redis_insertBusiness(geoHash,business)
      }
    }
    business
  }

  //从数据库获取商圈信息
  def redis_queryBusiness(geohash:String):String={
    val jedis = JedisConnectPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  //将商圈导出到数据库
  def redis_insertBusiness(geohash:String,business:String): Unit ={
    val jedis = JedisConnectPool.getConnection()
    jedis.set(geohash,business)
    jedis.close()
  }
}
