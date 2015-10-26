package com.mogobiz.cache.enrich

import java.text.MessageFormat

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import spray.client.pipelining._
import spray.http.{HttpEntity, HttpResponse}
import scala.collection.JavaConversions._
import com.sksamuel.elastic4s.ElasticDsl._

abstract class CacheConfig()

abstract class ParallelCacheConfig(maxClient: Integer) extends CacheConfig

case class NoConfig() extends CacheConfig

case class HttpConfig(protocol: String, host: String, port: Integer, uri: String, maxClient: Integer) extends ParallelCacheConfig(maxClient) {
  def getFullUri(): String = {
    s"${protocol}://${host}:${port}${uri}"
  }

  def getFullUri(params: List[String]): String = {
    val uriWithParams: String = MessageFormat.format(uri, params: _*)
    s"${protocol}://${host}:${port}${uriWithParams}"
  }
}

case class EsConfig(protocol: String, host: String, port: Integer, index: String, `type`: String, scrollTime: String, fields: List[String],
                    maxClient: Integer, batchSize: Integer = 100) extends
ParallelCacheConfig(maxClient) {

  def getEsIterator()(implicit actorSystem: ActorSystem): Iterator[Map[String, List[String]]] = {
    class EsIterator()(implicit actorSystem: ActorSystem) extends Iterator[Map[String, List[String]]] {

      import actorSystem.dispatcher

      implicit val pipeline: SendReceive = sendReceive

      private[this] var scrollId = {
        val fieldsRequested: String = fields.mkString(",")
        val urlScanScroll: String = s"${protocol}://${host}:${port}/${index}/${`type`}/_search?scroll=${scrollTime}&search_type=scan&fields=${fieldsRequested}&size=${batchSize}"
        val response: HttpResponse = pipeline(Get(urlScanScroll)).await
        ConfigFactory.parseString(response.entity.asString).getString("_scroll_id")
      }

      private[this] var dataIterator: Iterator[Map[String, List[String]]] = scrollData()

      override def hasNext: Boolean = {
        if (dataIterator.hasNext)
          true
        else {
          dataIterator = scrollData()
          dataIterator.hasNext
        }
      }

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