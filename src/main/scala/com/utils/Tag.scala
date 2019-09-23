package com.utils

trait Tag {
  def makeTag(row:Any*):List[(String,Int)]
}
