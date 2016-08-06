package com.mogobiz.cache.enrich

import java.net.URL

import com.mogobiz.cache.graph.CacheFlow
import com.mogobiz.cache.utils.UrlUtils
import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
  * Define helpers to typesafe's config
  */
object ConfigHelpers extends LazyLogging {

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
      SearchGuardConfig(active, username, password)
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
    def toPurgeConfig(): PurgeConfig = {
      val additionnalHeaders = if (config.hasPath("headers")) {
        config.getConfig("headers").entrySet().map(entry => {
          entry.getKey -> entry.getValue.unwrapped().toString
        }).toMap
      } else {
        Map.empty[String, String]
      }
      PurgeConfig(config.getOrElse("method", "GET").toUpperCase, config.getString("uri"), additionnalHeaders)
    }
  }

  /**
    *
    * @param url
    * @return a pair with url and a map that contains only one element which has the fields associated with.
    */
  private def getEsTypesFieldsAndFiltersFromUrl(url: String): Option[(String, Map[String, List[(String, String)]])] = {
    def getEsTypesFieldsAndFiltersFromUrl() = {
      Try {
        UrlUtils.extractUriIndicesVariablesNameAndFilter(url)
          .groupBy(_._1).mapValues(_.map(t => (t._2, t._3)))
      }
    }
    getEsTypesFieldsAndFiltersFromUrl() match {
      case Success(esIndicesFromUrl) => {
        Some((url, esIndicesFromUrl))
      }
      case Failure(e) => {
        logger.error(e.getMessage)
        logger.error(s"${url} is dropped")
        None
      }
    }
  }

  def getUri(validUrl: URL): String = {
    val hostAndPort = if (validUrl.getPort != -1)
      validUrl.getHost + ":" + validUrl.getPort
    else
      validUrl.getHost
    val split: Array[String] = validUrl.toExternalForm.split(Regex.quote(hostAndPort))
    if (split.length > 1)
      split(1)
    else
      ""
  }

  private def getHttpConfig(url: URL, maxClient: Integer): HttpConfig = {
    HttpConfig(
      url.getProtocol,
      "GET",
      url.getHost,
      if (url.getPort == -1) url.getDefaultPort else url.getPort,
      getUri(url),
      Map(),
      maxClient
    )
  }

  private def getEsConfig(esTypesFieldsAndFilters: Map[String, List[(String, String)]], esIndex: String): List[EsConfig] = {
    esTypesFieldsAndFilters.toList.map {
      case (index, fieldsAndFilters) => {
        val esFields: List[(String, Boolean)] = fieldsAndFilters.map { case (field, filter) => {
          val encodeField = if (filter == "encode")
            true
          else
            false
          (field, encodeField)
        }
        }
        val esConfig: Config = ConfigFactory.load().getConfig("mogobiz.cache.server.es")
        val searchguardConfig = if (esConfig.hasPath("searchguard")) {
          esConfig.getConfig("searchguard").toSearchGuardConfig()
        } else {
          SearchGuardConfig()
        }
        EsConfig(
          esConfig.getString("protocol"),
          esConfig.getString("host"),
          esConfig.getInt("port"),
          esIndex,
          index,
          "1m",
          esFields,
          searchguardConfig
        )
      }
    }
  }

  /**
    *
    * @param staticUrls list of static urls which can benefit with substitutions from the whole config
    * @return a config not resolved
    */
  def buildStaticUrlsConfig(esIndex: String, staticUrls: List[String]): List[CacheFlow] = {
    val sanitizedStaticUrls = staticUrls.filter(s => !s.isEmpty)
    if (sanitizedStaticUrls.isEmpty) {
      List[CacheFlow]()
    }
    else {
      sanitizedStaticUrls
        .flatMap(getEsTypesFieldsAndFiltersFromUrl)
        .flatMap {
          case (url, esTypesFieldsAndFilters) => {
            Try {
              new URL(UrlUtils.stripSpaceAndFiltersInVariable(url))
            } match {
              case Success(validUrl) => {
                val maxClient: Int = ConfigFactory.load().getInt("mogobiz.cache.server.httpServer.maxClient")
                // We handle only one indice at the moment
                esTypesFieldsAndFilters.isEmpty match {
                  case true => {
                    val httpConfig: HttpConfig = getHttpConfig(validUrl, maxClient)
                    Some(CacheFlow(None, httpConfig, NoConfig()))
                  }
                  case false => {
                    val esConfigList: List[EsConfig] = getEsConfig(esTypesFieldsAndFilters, esIndex)
                    val httpConfig: HttpConfig = getHttpConfig(validUrl, maxClient)
                    Some(CacheFlow(Some(esConfigList), httpConfig, NoConfig()))
                  }
                }
              }
              case Failure(e) => {
                logger.error(s"${url} is dropped", e)
                None
              }
            }
          }
        }
    }
  }
}
