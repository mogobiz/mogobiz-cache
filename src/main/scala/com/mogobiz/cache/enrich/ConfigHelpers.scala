package com.mogobiz.cache.enrich

import java.net.URL

import com.mogobiz.cache.exception.UnsupportedConfigException
import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import com.typesafe.scalalogging.LazyLogging
import spray.http.{HttpMethod, HttpMethods}

import scala.collection.JavaConversions._
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

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

    /**
      * @return a HttpConfig
      */
    def toPurgeConfig(): PurgeConfig = {
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
      PurgeConfig(method, config.getString("uri"), additionnalHeaders)
    }
  }

  /**
    *
    * @param url
    * @return a pair with url and a map that contains only one element which has the fields associated with.
    */
  private def getEsTypesFieldsAndFiltersFromUrl(url: String): Option[(String, Map[String, List[(String, String)]])] = {
    def getEsTypesFieldsAndFiltersFromUrl() = {
      val variables: Regex = "\\Q${\\E(.*?)\\Q}\\E".r
      Try {
        variables.findAllMatchIn(url)
          .map(matching => matching.subgroups(0))
          .map(subGroupMatching => {
            val (indice, fieldAndFilter) = subGroupMatching.span(_ != '.')
            val (field, filter) = if (fieldAndFilter.isEmpty) ("", "") else fieldAndFilter.tail.span(_ != '|')
            val (indiceTrimmed, fieldTrimmed, filterTrimmed) = (indice.trim, field.trim, if (filter.isEmpty) "" else filter.tail.trim)
            if (fieldTrimmed.isEmpty) {
              throw new IllegalArgumentException("The variable doesn't have any field " + subGroupMatching)
            } else {
              filterTrimmed match {
                case filter if filter.isEmpty => (indiceTrimmed, fieldTrimmed, filterTrimmed)
                case "encode" => (indiceTrimmed, fieldTrimmed, filterTrimmed)
                case f => throw new IllegalArgumentException(s"The filter ${f} doesn't exist")
              }
            }
          }).toList.groupBy(_._1).mapValues(_.map(t => (t._2, t._3)))
      }
    }
    getEsTypesFieldsAndFiltersFromUrl() match {
      case Success(esIndicesFromUrl) => {
        if (esIndicesFromUrl.size > 1) {
          logger.error(s"${url} use more than one ES type")
          None
        } else {
          Some((url, esIndicesFromUrl))
        }
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
    validUrl.toExternalForm.split(Regex.quote(hostAndPort))(1)
  }

  /**
    *
    * @param staticUrls list of static urls which can benefit with substitutions from the whole config
    * @return a config not resolved
    */
  def buildStaticUrlsConfig(esIndex: String, staticUrls: List[String]): Config = {
    val sanitizedStaticUrls = staticUrls.filter(s => !s.isEmpty)
    if (sanitizedStaticUrls.isEmpty) {
      ConfigFactory.empty()
    }
    else {
      val configList: List[Config] = sanitizedStaticUrls
        .flatMap {
          getEsTypesFieldsAndFiltersFromUrl
        }
        .flatMap { case (url, esTypesFieldsAndFilters) => {
          Try {
            new URL(url)
          } match {
            case Success(validUrl) => {
              // We handle only one indice at the moment
              esTypesFieldsAndFilters.toList.headOption match {
                case Some((index, fieldsAndFilters)) => {
                  val esFields: String = fieldsAndFilters.map { case (field, filter) => "\"" + field + "\"" }.toSet.mkString("[", ",", "]")
                  val encodeFields: String = fieldsAndFilters.map { case (field, filter) => {
                    if (filter == "encode") {
                      "true"
                    } else {
                      "false"
                    }
                  }
                  }.toSet.mkString("[", ",", "]")
                  Some(
                    s"""mogobiz.cache.uri.generic.process += {
                    input: {
                      index: ${esIndex}
                      type: "${index}"
                      fields: ${esFields}
                      encodeFields: ${encodeFields}
                      scroll: "5m"
                      size: 100
                      server: $${mogobiz.cache.server.es}
                    },
                    output: {
                      uri: "${getUri(validUrl)}"
                      server: {
                        protocol: "${validUrl.getProtocol}"
                        host: "${validUrl.getHost}"
                        port: "${if (validUrl.getPort == -1) validUrl.getDefaultPort else validUrl.getPort}"
                        maxClient: $${mogobiz.cache.server.httpServer.maxClient}
                      }
                    }
                  }""".stripMargin)
                }
                case _ => {
                  Some(
                    s"""mogobiz.cache.uri.generic.process += {output: {
                        uri: "${getUri(validUrl)}"
                        server: {
                              protocol: "${validUrl.getProtocol}"
                              host: "${validUrl.getHost}"
                              port: ${if (validUrl.getPort == -1) validUrl.getDefaultPort else validUrl.getPort}
                              maxClient: $${mogobiz.cache.server.httpServer.maxClient}
                            }
                      }}""".stripMargin)
                }
              }
            }
            case Failure(e) => {
              logger.error(s"${url} is dropped", e)
              None
            }
          }
        }
        }.map(s => ConfigFactory.parseString(s))
      configList.size match {
        case 0 => ConfigFactory.empty()
        case 1 => configList.head
        case _ => configList.reduce((c1, c2) => c2.withFallback(c1))
      }
    }
  }
}