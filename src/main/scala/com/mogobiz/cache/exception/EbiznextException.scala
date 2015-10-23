package com.mogobiz.cache.exception

case class UnsupportedTypeException(message:String) extends RuntimeException(message)
case class UnsupportedConfigException(message:String) extends RuntimeException(message)