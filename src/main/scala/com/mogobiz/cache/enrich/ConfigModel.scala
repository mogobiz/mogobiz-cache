package com.mogobiz.cache.enrich

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.mogobiz.cache.utils.{CustomSslConfiguration, UrlUtils}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import spray.client.pipelining._
import spray.http.{BasicHttpCredentials, HttpEntity, HttpMethod, HttpResponse}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, Future}

/**
  * Root type of all configs related to mogobiz-cache.
  */
abstract class CacheConfig()

/**
  * @param maxClient specify the parallelism
  */
abstract class ParallelCacheConfig(maxClient: Integer) extends CacheConfig

/**
  * No config found for an input. Having an output with NoConfig is currently impossible.
  */
case class NoConfig() extends CacheConfig

/**
  *
  * @param uri       uri to call, starting with /
  */
case class PurgeConfig(method: HttpMethod, uri: String, additionalHeaders: Map[String, String]) extends CacheConfig {
  def isByUri = uri contains "${uri}"
}
/**
  *
  * @param protocol  http or https
  * @param host      domain name or ip address
  * @param port      port, no default value is provided
  * @param uri       uri to call, starting with /
  * @param maxClient specify the parallelism
  */
case class HttpConfig(protocol: String, method: HttpMethod, host: String, port: Integer, uri: String, additionalHeaders: Map[String, String], maxClient: Integer) extends ParallelCacheConfig(maxClient) {


  val uriStringContext = UrlUtils.uriAsStringContext(uri)
  val uriVariables = UrlUtils.extractUriVariablesName(uri)

  /**
    * @return the full uri without replacing any values inside $uri
    */
  def getFullUri(): String = {
    s"${protocol}://${host}:${port}${uri}"
  }

  /**
    * @param params
    * @return the full uri with the uri interpolated.
    */
  def getFullUri(params: Map[String,String]): String = {
    s"${getProtocolHostPort}${getRelativeUri(params)}"
  }

  def getProtocolHostPort = {
    s"${protocol}://${host}:${port}"
  }

  /**
    * @param params
    * @return the full uri with the uri interpolated.
    */
  def getRelativeUri(params: Map[String,String]): String = {
    val values: List[String] = uriVariables.map(v => params.getOrElse(v,""))
    uriStringContext.s(values:_*)
  }
}

/**
  * ScrollTime and batchSize are somehow related. In fact the more element we get, the less call we do to elastic search and higher the scrollTime is.
  * Each batch extend the search context.
  *
  * @param protocol   http or https
  * @param host       domain name or ip address
  * @param port       port, no default value is provided
  * @param index      elastic search's index
  * @param `type`     elastic search's type
  * @param scrollTime search context opened period
  * @param fields     fields to select for each hits
  * @param batchSize  number of elements to retrieve after each call.
  */
case class EsConfig(protocol: String, host: String, port: Integer, index: String, `type`: String, scrollTime: String, fields: List[String], encodeFields: List[Boolean], searchGuardConfig: SearchGuardConfig, batchSize: Integer = 100) extends CacheConfig with LazyLogging {

  /**
    * @param actorSystem
    * @return custom iterator that scroll and scan elements from elastic search. Each element is a Map containing all fields requested that are available for each hits. That means a map.length can be different from fields.length.
    */
  def getEsIterator()(implicit actorSystem: ActorSystem): Iterator[Map[String, List[String]]] = {
    class EsIterator()(implicit actorSystem: ActorSystem) extends Iterator[Map[String, List[String]]] {

      import ConfigHelpers._
      import actorSystem.dispatcher

      val timeout = ConfigFactory.load().getOrElse("mogobiz.cache.timeout", 30).toLong

      val pipeline = CustomSslConfiguration.getPipeline(host, port, protocol.toLowerCase == "https")
      /**
        * Initial scrollId by asking elastic search to scan and scroll. No hits are returned by this request.
        */
      private[this] var scrollId = {
        val fieldsRequested: String = fields.mkString(",")
        val urlScanScroll: String = s"${protocol}://${host}:${port}/${index}/${`type`}/_search?scroll=${scrollTime}&search_type=scan&fields=${fieldsRequested}&size=${batchSize}"
        val scanScrollHttpRequest = if (searchGuardConfig.active) {
          Get(urlScanScroll) ~> addCredentials(BasicHttpCredentials(searchGuardConfig.username, searchGuardConfig.password))
        } else {
          Get(urlScanScroll)
        }
        val httpResponseF: Future[HttpResponse] = pipeline.flatMap(p => p(scanScrollHttpRequest))
        val response: HttpResponse = getFuture(httpResponseF)
        if (response.status.isFailure) {
          logger.error(s"Failed to retrieve fields [${fieldsRequested}] from index ${index}")
        }
        ConfigFactory.parseString(response.entity.asString).getString("_scroll_id")
      }

      /**
        * Iterator of the different hits.
        */
      private[this] var dataIterator: Iterator[Map[String, List[String]]] = scrollData()

      override def hasNext: Boolean = {
        if (dataIterator.hasNext)
          true
        else {
          // if there is no more data, update the iterator first with new data. If no element is retrieved, hasNext on the iterator will return false.
          dataIterator = scrollData()
          dataIterator.hasNext
        }
      }

      /**
        * @return an iterator of the different hits. Call ES with scroll_id and build a map containing the fields.
        */
      private def scrollData(): Iterator[Map[String, List[String]]] = {
        val urlScanScroll = s"${protocol}://${host}:${port}/_search/scroll?scroll=${scrollTime}"
        val scanScrollHttpRequest = if (searchGuardConfig.active) {
          Post(urlScanScroll).withEntity(HttpEntity(scrollId)) ~> addCredentials(BasicHttpCredentials(searchGuardConfig.username, searchGuardConfig.password))
        } else {
          Post(urlScanScroll).withEntity(HttpEntity(scrollId))
        }
        val httpResponseF: Future[HttpResponse] = pipeline.flatMap(p => p(scanScrollHttpRequest))
        val response: HttpResponse = getFuture(httpResponseF)
        val config: Config = ConfigFactory.parseString(response.entity.asString)
        scrollId = config.getString("_scroll_id")
        val hits = config.getConfigList("hits.hits")
        hits.map(c => {
          import ConfigHelpers._
          if (c.hasPath("fields")) {
            val fieldsConfig: Config = c.getConfig("fields")
            fields.flatMap(f => {
              val quotedField = "\"" + f + "\""
              if (fieldsConfig.hasPath(quotedField)) {
                Some(f -> fieldsConfig.getAsList[AnyRef](quotedField).map(_.toString))
              } else {
                None
              }
            }).toMap
          } else {
            Map.empty[String, List[String]]
          }
        }).toIterator
      }

      override def next(): Map[String, List[String]] = dataIterator.next()

      private def getFuture[T](awaitable: Awaitable[T]) = {
        Await.result(awaitable, Duration(timeout, TimeUnit.SECONDS))
      }
    }
    new EsIterator()
  }

}

/**
  *
  * @param active indicate that search guard is enabled
  * @param username
  * @param password
  */
final case class SearchGuardConfig(active: Boolean = false, username: String = "", password: String = "")