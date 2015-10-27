package com.mogobiz.cache.enrich

import com.typesafe.config.{Config, ConfigValueType}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._

/**
 * Define helpers to typesafe's config
 */
object ConfigHelpers {

  /**
   * Implicit class for typesafe's config
   * @param config
   */
  implicit class RichConfig(config: Config) {

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
      val index: String = config.getOrElse("index", rootConfig.getString("mogobiz.cache.store"))
      val tpe = config.getString("type")
      val fields: List[String] = config.getAsList("fields")
      val scrollTime = config.getOrElse("scroll", "1m")
      val encodeFields: List[Boolean] = if (config.hasPath("encodeFields")) {
        val booleans: List[Boolean] = config.getAsList[Boolean]("encodeFields")
        //feel the list to match at least fields size.
        if (booleans.length < fields.length) {
          (booleans.length until fields.length).foldLeft(booleans.reverse)((list,i) => true :: list).reverse
        } else {
          booleans
        }
      }else {
        fields.map(f => true)
      }
      EsConfig(protocol, host, port, index, tpe, scrollTime, fields, encodeFields, esConfig.getOrElse("maxClient", 10))
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
     * @return a HttpConfig
     */
    def toHttpConfig(): HttpConfig = {
      val httpServer: Config = config.getConfig("server")
      HttpConfig(httpServer.getString("protocol"), httpServer.getString("host"), httpServer.getInt("port"), config.getString("uri"), httpServer.getOrElse("maxClient", 10))
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
    def getOrElse[A](path: String, default: A)(implicit tag: TypeTag[A]): A = {
      if (config.hasPath(path)) {
        config
          .getAnyRef(path)
          .asInstanceOf[A]
      } else {
        default
      }
    }
  }

}