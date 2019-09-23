package com.utils

object String2Type {

  def toInt(string: String):Int={
   try{
    string.toInt
   }catch{
    case _:Exception=>0
   }
  }
  def toDouble(str: String):Double ={
    try{
      str.toDouble
    }catch {
      case _ :Exception =>0.0
    }
  }

}
