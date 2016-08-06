package com.mogobiz.cache.enrich

import java.util.concurrent.TimeUnit
import javax.xml.ws.http.HTTPException

import akka.actor.ActorSystem
import com.mogobiz.cache.utils.{CustomSslOkHttpClient, UrlUtils}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import okhttp3._

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable}

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
case class PurgeConfig(method: String, uri: String, additionalHeaders: Map[String, String]) extends CacheConfig {
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
case class HttpConfig(protocol: String, method: String, host: String, port: Integer, uri: String, additionalHeaders: Map[String, String], maxClient: Integer) extends ParallelCacheConfig(maxClient) {


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
  * @param fieldsAndEncode     fields to select for each hits and boolean to check if it needs encoding
  * @param batchSize  number of elements to retrieve after each call.
  */
case class EsConfig(protocol: String, host: String, port: Integer, index: String, `type`: String, scrollTime: String, fieldsAndEncode: List[(String, Boolean)], searchGuardConfig: SearchGuardConfig, batchSize: Integer = 100) extends CacheConfig with LazyLogging {

  val distinctFields = fieldsAndEncode.map{case(field, encode) => field}.toSet
  val fieldsPathAndEncode = fieldsAndEncode.map{case(field, encode) => (`type` + '.' + field, encode)}

  /**
    * @param actorSystem
    * @return custom iterator that scroll and scan elements from elastic search. Each element is a Map containing all fields requested that are available for each hits. That means a map.length can be different from fields.length.
    */
  def getEsIterator()(implicit actorSystem: ActorSystem): Iterator[Map[String, List[String]]] = {
    class EsIterator()(implicit actorSystem: ActorSystem) extends Iterator[Map[String, List[String]]] {

      import ConfigHelpers._

      val timeout = ConfigFactory.load().getOrElse("mogobiz.cache.timeout", 30).toLong

      val client = if(searchGuardConfig.active) {
        CustomSslOkHttpClient.getClient(Some(new Authenticator {
          override def authenticate(route: Route, response: Response): Request = {
            response.request().newBuilder()
              .header("Authorization", Credentials.basic(searchGuardConfig.username, searchGuardConfig.password))
              .build()
          }
        }))
      } else {
        CustomSslOkHttpClient.getClient()
      }

      /**
        * Initial scrollId by asking elastic search to scroll. No hits are returned by this request.
        */
      private[this] var (scrollId, dataIterator: Iterator[Map[String, List[String]]]) = {
        val fieldsRequested: String = distinctFields.mkString(",")
        val urlScroll: String = s"${protocol}://${host}:${port}/${index}/${`type`}/_search?scroll=${scrollTime}&fields=${fieldsRequested}&size=${batchSize}"
        val scrollHttpRequest = new Request.Builder().url(urlScroll).build()
        val response: Response = client.newCall(scrollHttpRequest).execute()
        val responseStr: String = response.body().string()
        if (!response.isSuccessful) {
          logger.error(s"Failed to retrieve fields [${fieldsRequested}] from index ${index} and type ${`type`}")
          logger.error(s"Attempted to reach ${urlScroll} with failure.")
          logger.error(s"Http response status ${response.code} with body : \n ${responseStr}")
          throw new HTTPException(response.code)
        } else {
          logger.debug(s"ES Response :\n${responseStr}")
        }
        parseResponse(responseStr)
      }

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
        val urlScroll = s"${protocol}://${host}:${port}/_search/scroll?scroll=${scrollTime}"
        val scrollHttpRequest = new Request.Builder().url(urlScroll).post(RequestBody.create(MediaType.parse("text/plain;charset=utf-8"), scrollId)).build()
        val response: Response = client.newCall(scrollHttpRequest).execute()
        val responseStr: String = response.body().string()
        if (!response.isSuccessful) {
          logger.error(s"Failed to scroll from index ${index} and type ${`type`}")
          logger.error(s"Http response status ${response.code} with body : \n ${responseStr}")
          throw new HTTPException(response.code)
        } else {
          logger.debug(s"ES Response :\n${responseStr}")
          val (scrollIdInResponse, iterator) = parseResponse(responseStr)
          scrollId = scrollIdInResponse
          iterator
        }
      }

      private def parseResponse(responseStr: String): (String, Iterator[Map[String, List[String]]]) = {
        val config: Config = ConfigFactory.parseString(responseStr)
        val scrollId = config.getString("_scroll_id")
        val hits = config.getConfigList("hits.hits")
        val iterator: Iterator[Map[String, List[String]]] = hits.map(c => {
          import ConfigHelpers._
          if (c.hasPath("fields")) {
            val fieldsConfig: Config = c.getConfig("fields")
            distinctFields.flatMap { f => {
              val quotedField = "\"" + f + "\""
              if (fieldsConfig.hasPath(quotedField)) {
                Some(`type` + '.' + f -> fieldsConfig.getAsList[AnyRef](quotedField).map(_.toString))
              } else {
                None
              }
            }
            }.toMap
          } else {
            Map.empty[String, List[String]]
          }
        }).toIterator
        (scrollId, iterator)
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