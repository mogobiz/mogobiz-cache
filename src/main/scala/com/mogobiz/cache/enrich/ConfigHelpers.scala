package com.mogobiz.cache.enrich

import com.mogobiz.cache.exception.UnsupportedTypeException
import com.typesafe.config.{Config, ConfigValueType}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

object ConfigHelpers {

  implicit class RichConfig(config: Config) {

    def toEsConfig()(implicit rootConfig: Config): EsConfig = {
      val esConfig: Config = rootConfig.getConfig("mogobiz.cache.server.es")
      val protocol = esConfig.getString("protocol")
      val host = esConfig.getString("host")
      val port: Integer = esConfig.getInt("port")
      val index: String = config.getOrElse("index", rootConfig.getString("mogobiz.cache.store"))
      val tpe = config.getString("type")
      val fields: List[String] = config.getAsList("fields")
      val scrollTime = config.getOrElse("scroll", "5m")
      val size = config.getOrElse("size", 100)
      EsConfig(protocol, host, port, index, tpe, scrollTime, fields, esConfig.getInt("maxClient"), size)
    }

    def getOrElse[A](path: String, default: A)(implicit tag: TypeTag[A]): A = {
      if (config.hasPath(path)) {
        tag.tpe match {
          case TypeRef(_, c, _) => c match {
            case aClass if (aClass.isClass && aClass.asClass == runtimeMirror(this.getClass.getClassLoader).classSymbol(classOf[String])) => config
              .getAnyRef(path)
              .asInstanceOf[A]
            case aClass => throw UnsupportedTypeException(aClass.asClass.toType + " not supported " + typeOf[String])
          }
        }
      } else {
        default
      }
    }

    def getAsList[A](path: String): List[A] = {
      config.getValue(path).valueType() match {
        case ConfigValueType.LIST => {
          config.getAnyRefList(path).toList.asInstanceOf[List[A]]
        }
        case _ => List(config.getAnyRef(path)).asInstanceOf[List[A]]
      }
    }

    def toApiHttpConfig()(implicit rootConfig: Config): HttpConfig = {
      val cacheApiServer: Config = rootConfig.getConfig("mogobiz.cache.server.cache.api")
      toHttpConfig(cacheApiServer)
    }

    def toJahiaHttpConfig()(implicit rootConfig: Config): HttpConfig = {
      val cacheJahiaServer: Config = rootConfig.getConfig("mogobiz.cache.server.cache.jahia")
      toHttpConfig(cacheJahiaServer)
    }

    private def toHttpConfig(cacheServer: Config) = {
      HttpConfig(cacheServer.getString("protocol"), cacheServer.getString("host"), cacheServer.getInt("port"), config.getString("uri"), cacheServer.getInt("maxClient"))
    }
  }

}