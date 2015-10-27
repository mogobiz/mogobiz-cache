package com.mogobiz.cache.graph

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.mogobiz.cache.enrich.{CacheConfig, EsConfig, HttpConfig, NoConfig}
import com.mogobiz.cache.exception.UnsupportedConfigException
import com.typesafe.scalalogging.LazyLogging
import spray.client.pipelining
import spray.client.pipelining._
import spray.http.HttpHeaders.RawHeader
import spray.http._
import spray.httpx.RequestBuilding

import scala.concurrent.Future

case class CacheFlow(source: CacheConfig, sink: CacheConfig)

object CacheGraph extends LazyLogging with RequestBuilding{

  /**
   *
   * @param cacheFlow
   * @param actorSystem
   * @param actorMaterializer
   * @return return a runnable graph base on the flow described.
   */
  def cacheRunnableGraph(cacheFlow: CacheFlow)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer):
  RunnableGraph[Future[Unit]] = {
    import actorSystem.dispatcher
    val pipeline = sendReceive
    cacheFlow match {
      // Execute a get request
      case CacheFlow(_: NoConfig, h: HttpConfig) => Source.single(1).mapAsyncUnordered(h.maxClient) { i =>
        val fullUri: String = h.getFullUri()
        logger.info(s"Requesting ${h.method} ${fullUri}")
        val request: HttpRequest = HttpRequest(h.method,fullUri,buildHeaders(h))
        pipeline(request).flatMap(httpResponse => Future {
          (fullUri, httpResponse)
        })
      }.map(logHttpResponseFailure).toMat(Sink.ignore)(Keep.right)
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
        // encode fields
        .map {fields =>
          fields.zip(c.encodeFields).map {
            case (f, true) => URLEncoder.encode(f,StandardCharsets.UTF_8.name())
            case (f, _) => f
          }
        }
        .mapAsyncUnordered(h.maxClient) { (fields: List[String]) => {
          val fullUri: String = h.getFullUri(fields)
          logger.info(s"Requesting ${h.method} ${fullUri}")
          val request: HttpRequest = HttpRequest(h.method,fullUri,buildHeaders(h))
          pipeline(request).flatMap(httpResponse => Future {
            (fullUri, httpResponse)
          })
        }
        }.map(logHttpResponseFailure).toMat(Sink.ignore)(Keep.right)
      case CacheFlow(source, sink) => throw UnsupportedConfigException(s"Input[${source.getClass.getName}] to Sink[${sink.getClass.getName}] not supported")
    }
  }

  def buildHeaders(h: HttpConfig): List[RawHeader] = {
    h.additionalHeaders.map(h => RawHeader(h._1, h._2)).toList
  }

  /**
   * Log the response in failure state.
   * @return the current HttpResponse.
   */
  def logHttpResponseFailure: PartialFunction[(String, HttpResponse), HttpResponse] = {
    case (fullUri, httpResponse) =>
      if (httpResponse.status.isFailure) {
        logger.warn(s"Request ${fullUri} failed with status ${httpResponse.status}")
      }
      httpResponse
  }
}
