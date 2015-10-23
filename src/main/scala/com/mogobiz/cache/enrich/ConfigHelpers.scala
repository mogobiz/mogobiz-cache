package com.mogobiz.cache.enrich

import java.text.MessageFormat

import akka.actor.ActorSystem
import com.mogobiz.cache.exception.UnsupportedTypeException
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.{search, _}
import com.sksamuel.elastic4s.streams.ReactiveElastic._
import com.sksamuel.elastic4s.streams.ScrollPublisher
import com.typesafe.config.{Config, ConfigValueType}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

abstract class CacheConfig()

abstract class ParallelCacheConfig(maxClient:Integer) extends CacheConfig

case class NoConfig() extends CacheConfig

case class EsConfig(host: String, tcpPort: Integer, index: String, `type`: String, scrollTime: String, fields: List[String], maxClient:Integer) extends
ParallelCacheConfig(maxClient) {
  def getEsPublisher()(implicit actorSystem: ActorSystem): ScrollPublisher = {
    val client: ElasticClient = ElasticClient.remote(host, tcpPort)
    client.publisher(search in index / `type` fields (fields: _*) scroll scrollTime)
  }
}

case class HttpConfig(protocol: String, host: String, port: Integer, uri: String, maxClient:Integer) extends ParallelCacheConfig(maxClient) {
  def getFullUri():String = {
    s"${protocol}://${host}:${port}${uri}"
  }

  def getFullUri(params:List[String]):String = {
    val uriWithParams: String = MessageFormat.format(uri,params:_*)
    s"${protocol}://${host}:${port}${uriWithParams}"
  }
}

object ConfigHelpers {

  implicit class RichConfig(config: Config) {

    def toEsConfig()(implicit rootConfig: Config): EsConfig = {
      val esConfig: Config = rootConfig.getConfig("mogobiz.cache.server.es")
      val host = esConfig.getString("host")
      val port: Integer = esConfig.getInt("port.tcp")
      val index: String = config.getOrElse("index", rootConfig.getString("mogobiz.cache.store"))
      val tpe = config.getString("type")
      val fields: List[String] = config.getAsList("fields")
      val scrollTime = config.getOrElse("scroll", "5m")
      EsConfig(host, port, index, tpe, scrollTime, fields,esConfig.getInt("maxClient"))
    }

    def toApiHttpConfig()(implicit rootConfig:Config):HttpConfig = {
      val cacheApiServer: Config = rootConfig.getConfig("mogobiz.cache.server.cache.api")
      toHttpConfig(cacheApiServer)
    }

    def toJahiaHttpConfig()(implicit rootConfig:Config):HttpConfig = {
      val cacheJahiaServer: Config = rootConfig.getConfig("mogobiz.cache.server.cache.jahia")
      toHttpConfig(cacheJahiaServer)
    }

    private def toHttpConfig(cacheServer:Config) = {
      HttpConfig(cacheServer.getString("protocol"), cacheServer.getString("host"), cacheServer.getInt("port"), config.getString("uri"), cacheServer.getInt("maxClient"))
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
  }

}