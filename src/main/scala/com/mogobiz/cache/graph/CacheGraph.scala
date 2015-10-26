package com.mogobiz.cache.graph

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.mogobiz.cache.enrich.{CacheConfig, EsConfig, HttpConfig, NoConfig}
import com.mogobiz.cache.exception.UnsupportedConfigException
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import spray.client.pipelining._

import scala.concurrent.Future

case class CacheFlow(source: CacheConfig, sink: CacheConfig)

object CacheGraph extends LazyLogging{

  def cacheRunnableGraph(cacheFlow: CacheFlow)(implicit rootConfig: Config, actorSystem: ActorSystem, actorMaterializer: ActorMaterializer):
  RunnableGraph[Future[Unit]] = {
    import actorSystem.dispatcher
    cacheFlow match {
      // Execute a get request
      case CacheFlow(_: NoConfig, h: HttpConfig) => Source.single(1).mapAsyncUnordered(h.maxClient) { i =>
        val pipeline: SendReceive = sendReceive
        logger.info(s"Retrieving data from ${h.getFullUri()}")
        pipeline(Get(h.getFullUri()))
      }.toMat(Sink.ignore)(Keep.right)
      // Retrieve data from ES and then execute a get request using input's data.
      case CacheFlow(c: EsConfig, h: HttpConfig) => Source(c.getEsIterator().toStream).map ( hit => {
        c.fields.map(hit.getOrElse(_, List()))
      })
        //remove all the hits that doesn't have all fields
        .filter{ fields =>
            val result: Boolean = fields.filter(!_.isEmpty).length == c.fields.length
            if(!result){
              logger.warn("One hit doesn't have all required fields")
            }
            result
        }
        // currently keep the first value in case where a field has multiple value.
        .map{ fields =>
          fields.map(l => l(0))
        }
        .mapAsyncUnordered(h.maxClient) { (fields: List[String]) => {
        val pipeline: SendReceive = sendReceive
        logger.info(s"Retrieving data from ${h.getFullUri(fields)}")
        pipeline(Get(h.getFullUri(fields)))
      }
      }.toMat(Sink.ignore)(Keep.right)
      case c => throw UnsupportedConfigException(c.getClass.getName + " not supported")
    }
  }
}
