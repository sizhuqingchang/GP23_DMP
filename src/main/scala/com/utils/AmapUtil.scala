package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer
object AmapUtil {
  def getBusiFromAmap(long:Double,lat:Double):String={
    val location: String = long+","+lat
    val url: String = "https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=59283c76b065e4ee401c2b8a4fde8f8b"

    val jsonstr=HttpUtil.get(url)
    val jSONObject: JSONObject = JSON.parseObject(jsonstr)

    val status=jSONObject.getJSONObject("status")
    if(status == 0) return ""

    // 如果不为空
    val jSONObject1 = jSONObject.getJSONObject("regeocode")
    if(jSONObject == null) return ""
    val jsonObject2 = jSONObject1.getJSONObject("addressComponent")
    if(jsonObject2 == null) return ""
    val jSONArray = jsonObject2.getJSONArray("businessAreas")
    if(jSONArray == null) return  ""

    val buffer: ListBuffer[String] = collection.mutable.ListBuffer[String]()
    for (item <- jSONArray.toArray()){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        buffer.append(name)
      }
    }
    buffer.mkString(",")
  }


}
