package com.mogobiz.cache.enrich

import java.text.MessageFormat

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.config.{Config, ConfigFactory}
import spray.client.pipelining._
import spray.http.{HttpEntity, HttpResponse}

import scala.collection.JavaConversions._

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
 * @param protocol http or https
 * @param host domain name or ip address
 * @param port port, no default value is provided
 * @param uri uri to call, starting with /
 * @param maxClient specify the parallelism
 */
case class HttpConfig(protocol: String, host: String, port: Integer, uri: String, maxClient: Integer) extends ParallelCacheConfig(maxClient) {
  /**
   * @return the full uri without replacing any values inside $uri
   */
  def getFullUri(): String = {
    s"${protocol}://${host}:${port}${uri}"
  }

  /**
   * @param params
   * @return the full uri with the uri interpolated with params.
   */
  def getFullUri(params: List[String]): String = {
    val uriWithParams: String = MessageFormat.format(uri, params: _*)
    s"${protocol}://${host}:${port}${uriWithParams}"
  }
}

/**
 * ScrollTime and batchSize are somehow related. In fact the more element we get, the less call we do to elastic search and higher the scrollTime is.
 * Each batch extend the search context.
 *
 * @param protocol http or https
 * @param host domain name or ip address
 * @param port port, no default value is provided
 * @param index elastic search's index
 * @param `type` elastic search's type
 * @param scrollTime search context opened period
 * @param fields fields to select for each hits
 * @param maxClient specify the parallelism
 * @param batchSize number of elements to retrieve after each call.
 */
case class EsConfig(protocol: String, host: String, port: Integer, index: String, `type`: String, scrollTime: String, fields: List[String],
                    maxClient: Integer, batchSize: Integer = 100) extends ParallelCacheConfig(maxClient) {

  /**
   * @param actorSystem
   * @return custom iterator that scroll and scan elements from elastic search. Each element is a Map containing all fields requested that are available for each hits. That means a map.length can be different from fields.length.
   */
  def getEsIterator()(implicit actorSystem: ActorSystem): Iterator[Map[String, List[String]]] = {
    class EsIterator()(implicit actorSystem: ActorSystem) extends Iterator[Map[String, List[String]]] {

      import actorSystem.dispatcher

      implicit val pipeline: SendReceive = sendReceive

      /**
       * Initial scrollId by asking elastic search to scan and scroll. No hits are returned by this request.
       */
      private[this] var scrollId = {
        val fieldsRequested: String = fields.mkString(",")
        val urlScanScroll: String = s"${protocol}://${host}:${port}/${index}/${`type`}/_search?scroll=${scrollTime}&search_type=scan&fields=${fieldsRequested}&size=${batchSize}"
        val response: HttpResponse = pipeline(Get(urlScanScroll)).await
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
        val response: HttpResponse = pipeline(Post(s"${protocol}://${host}:${port}/_search/scroll?scroll=${scrollTime}").withEntity(HttpEntity(scrollId))).await
        val config: Config = ConfigFactory.parseString(response.entity.asString)
        scrollId = config.getString("_scroll_id")
        val hits = config.getConfigList("hits.hits")
        hits.map(c => {
          import ConfigHelpers._
          if (c.hasPath("fields")) {
            val fieldsConfig: Config = c.getConfig("fields")
            fields.flatMap(f => {
              if (fieldsConfig.hasPath(f)) {
                Some(f -> fieldsConfig.getAsList[AnyRef](f).map(_.toString))
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
    }
    new EsIterator()
  }

}