package com.utils

object PCtUtils{

  def PrcList(requestmode:Int,processnode:Int):List[Double]={
    if(requestmode==1 && processnode==1){
      List(1,0,0)
    }else if(requestmode==1 && processnode==2){
      List(1,1,0)
    }else if(requestmode==1 && processnode==3){
      List(1,1,1)
    }else {
      List(0,0,0)
    }
  }

  def clickPt(requestmode:Int,iseffective:Int):List[Double]={
    if(requestmode==2 && iseffective==1){
      List(1,0)
    }else if(requestmode==3 && iseffective==1){
      List(0,1)
    }else {
      List(0,0)
    }
  }

  def adPt(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,winprice:Double,adpayment:Double):List[Double]={
    if(iseffective==1 && isbilling==1 && isbid==1){
      if(iseffective ==1 && isbilling ==1 && iswin ==1 && adorderid !=0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    } else {
      List(0,0,0,0)
    }
  }

  def Devtype(devicetype: Int):String={
    if(devicetype==1){
      "手机"
    }else if(devicetype==2){
      "平板"
    }else{
      "其他"
    }
  }

  //设备类型 （1：android 2：ios 3：wp）
  def osType(client:Int):String={

    if(client==1){
      "android"
    }else if(client==2){
      "ios"
    }else if(client==3){
      "wp"
    } else {
      "other"
    }
  }
}
