package com.mogobiz.cache.enrich

import com.mogobiz.cache.exception.UnsupportedConfigException
import com.typesafe.config.{Config, ConfigValueType}
import spray.http.{HttpMethods, HttpMethod}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/**
  * Define helpers to typesafe's config
  */
object ConfigHelpers {

  /**
    * Implicit class for typesafe's config
    *
    * @param config
    */
  implicit class RichConfig(config: Config) {

    /**
      *
      * @return build a SearchGuardConfig instance from a config file.
      */
    def toSearchGuardConfig(): SearchGuardConfig = {
      val active: Boolean = config.getBoolean("active")
      val username: String = config.getString("username")
      val password: String = config.getString("password")
      SearchGuardConfig(active,username,password)
    }

    /**
      *
      * @param rootConfig needed for the default value of the store.
      * @return build an EsConfig instance from a config file.
      */
    def toEsConfig()(implicit rootConfig: Config): EsConfig = {
      val esConfig: Config = config.getConfig("server")
      val protocol = esConfig.getString("protocol")
      val host = esConfig.getString("host")
      val port: Integer = esConfig.getInt("port")
      val searchguardConfig = if (esConfig.hasPath("searchguard")) {
        esConfig.getConfig("searchguard").toSearchGuardConfig()
      } else {
        SearchGuardConfig()
      }
      val index: String = config.getOrElse("index", rootConfig.getString("mogobiz.cache.store"))
      val tpe = config.getString("type")
      val fields: List[String] = config.getAsList("fields")
      val scrollTime = config.getOrElse("scroll", "1m")
      val encodeFields: List[Boolean] = if (config.hasPath("encodeFields")) {
        val booleans: List[Boolean] = config.getAsList[Boolean]("encodeFields")
        //feel the list to match at least fields size.
        if (booleans.length < fields.length) {
          (booleans.length until fields.length).foldLeft(booleans.reverse)((list, i) => true :: list).reverse
        } else {
          booleans
        }
      } else {
        fields.map(f => true)
      }
      EsConfig(protocol, host, port, index, tpe, scrollTime, fields, encodeFields, searchguardConfig, esConfig.getOrElse("maxClient", 10))
    }

    /**
      * @param path
      * @tparam A
      * @return a list of elements even if it's a single element.
      */
    def getAsList[A](path: String): List[A] = {
      config.getValue(path).valueType() match {
        case ConfigValueType.LIST => {
          config.getAnyRefList(path).toList.asInstanceOf[List[A]]
        }
        case _ => List(config.getAnyRef(path)).asInstanceOf[List[A]]
      }
    }

    /**
      *
      * @param path
      * @param default
      * @param tag
      * @tparam A
      * @return the default value if there is no path defined in the configuration file or the value at the given path.
      *         Be careful of the default value which define the type to get. The config file has to match this type.
      */
    def getOrElse[A](path: String, default: => A)(implicit tag: TypeTag[A]): A = {
      if (config.hasPath(path)) {
        config
          .getAnyRef(path)
          .asInstanceOf[A]
      } else {
        default
      }
    }

    /**
      * @return a HttpConfig
      */
    def toHttpConfig(): HttpConfig = {
      val httpServer: Config = config.getConfig("server")
      val method: HttpMethod = HttpMethods.getForKey(config.getOrElse("method", "GET").toUpperCase) match {
        case Some(httpMethod) => httpMethod
        case _ => throw UnsupportedConfigException(config.getString("method").toUpperCase + " HTTP method is unknown")
      }
      val additionnalHeaders = if (config.hasPath("headers")) {
        config.getConfig("headers").entrySet().map(entry => {
          entry.getKey -> entry.getValue.unwrapped().toString
        }).toMap
      } else {
        Map.empty[String, String]
      }
      HttpConfig(httpServer.getString("protocol"), method, httpServer.getString("host"), httpServer.getInt("port"), config.getString("uri"), additionnalHeaders, httpServer.getOrElse("maxClient", 10))
    }
  }

}