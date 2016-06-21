package com.mogobiz.cache.graph

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.mogobiz.cache.enrich.{CacheConfig, EsConfig, HttpConfig, NoConfig}
import com.mogobiz.cache.exception.UnsupportedConfigException
import com.mogobiz.cache.utils.{CustomSslConfiguration, HeadersUtils}
import com.typesafe.scalalogging.LazyLogging
import spray.http._
import spray.httpx.RequestBuilding

import scala.concurrent.Future

case class CacheFlow(source: CacheConfig, sink: CacheConfig, purgeConfig: CacheConfig)

object CacheGraph extends LazyLogging with RequestBuilding {

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
    cacheFlow match {
      // Execute a get request
      case CacheFlow(_: NoConfig, h: HttpConfig, p: HttpConfig) => {
        val pipeline = CustomSslConfiguration.getPipeline(h.host, h.port, h.protocol.toLowerCase == "https")
        val graphStart = if (p.uri == "{0}")
          Source.single(1).mapAsyncUnordered(p.maxClient) { i =>
            val fullUri: String = h.getFullUri()
            logger.info(s"Requesting ${p.method} ${fullUri}")
            val request: HttpRequest = HttpRequest(p.method, fullUri, buildHeaders(p))
            pipeline.flatMap(p => p(request)).map(httpResponse => (fullUri, httpResponse))
          }.map(logHttpResponseFailure)
        else
          Source.single(1)
        graphStart.mapAsyncUnordered(h.maxClient) { i =>
          val fullUri: String = h.getFullUri()
          logger.info(s"Requesting ${h.method} ${fullUri}")
          val request: HttpRequest = HttpRequest(h.method, fullUri, buildHeaders(h))
          pipeline.flatMap(p => p(request)).map(httpResponse => (fullUri, httpResponse))
        }.map(logHttpResponseFailure)
          .toMat(Sink.ignore)(Keep.right)
      }
      // Retrieve data from ES and then execute a get request using input's data.
      case CacheFlow(c: EsConfig, h: HttpConfig, p: HttpConfig) => {
        val pipeline = CustomSslConfiguration.getPipeline(h.host, h.port, h.protocol.toLowerCase == "https")
        val graphStart: Source[List[String], Unit] = Source(c.getEsIterator().toStream).map(hit => {
          c.fields.map(hit.getOrElse(_, List()))
        })
          //remove all the hits that doesn't have all fields
          .filter { fields =>
          val result: Boolean = fields.filter(!_.isEmpty).length == c.fields.length
          if (!result) {
            logger.warn("One hit doesn't have all required fields")
          }
          result
        }
          // currently keep the first value in case where a field has multiple value.
          .map { fields =>
          fields.map(l => l(0))
        }
          // encode fields
          .map { fields =>
          fields.zip(c.encodeFields).map {
            case (f, true) => URLEncoder.encode(f, StandardCharsets.UTF_8.name())
            case (f, _) => f
          }
        }
        val middleGraph = if (p.uri == "{0}") {
          graphStart.mapAsyncUnordered(p.maxClient) { (fields: List[String]) => {
            val fullUri: String = h.getFullUri(fields)
            logger.info(s"Requesting ${p.method} ${fullUri}")
            val request: HttpRequest = HttpRequest(p.method, fullUri, buildHeaders(p))
            pipeline.flatMap(p => p(request)).flatMap(httpResponse => Future {
              (fullUri, httpResponse, fields)
            })
          }
          }.map(logHttpResponseFailureAndReturnFields)
        } else {
          graphStart
        }
        middleGraph.mapAsyncUnordered(h.maxClient) { (fields: List[String]) => {
          val fullUri: String = h.getFullUri(fields)
          logger.info(s"Requesting ${h.method} ${fullUri}")
          val request: HttpRequest = HttpRequest(h.method, fullUri, buildHeaders(h))
          pipeline.flatMap(p => p(request)).flatMap(httpResponse => Future {
            (fullUri, httpResponse)
          })
        }
        }.map(logHttpResponseFailure).toMat(Sink.ignore)(Keep.right)
      }
        // Execute the purge request
      case CacheFlow(_: NoConfig, _: NoConfig, p: HttpConfig) => {
        val pipeline = CustomSslConfiguration.getPipeline(p.host, p.port, p.protocol.toLowerCase == "https")
          Source.single(1).mapAsyncUnordered(p.maxClient) { i =>
            val fullUri: String = p.getFullUri
            logger.info(s"Requesting ${p.method} ${fullUri}")
            val request: HttpRequest = HttpRequest(p.method, fullUri, buildHeaders(p))
            pipeline.flatMap(p => p(request)).map(httpResponse => (fullUri, httpResponse))
          }.map(logHttpResponseFailure).toMat(Sink.ignore)(Keep.right)
      }
      case CacheFlow(source, sink, purge) => throw UnsupportedConfigException(s"Input[${source.getClass.getName}] to Sink[${sink.getClass.getName}] not supported")
    }
  }


  def buildHeaders(h: HttpConfig): List[HttpHeader] = {
    h.additionalHeaders.map(HeadersUtils.buildHeader).toList
  }

  /**
    * Log the response in failure state.
    *
    * @return the current HttpResponse.
    */
  def logHttpResponseFailure: PartialFunction[(String, HttpResponse), HttpResponse] = {
    case (fullUri, httpResponse) =>
      if (httpResponse.status.isFailure) {
        logger.warn(s"Request ${fullUri} failed with status ${httpResponse.status}")
      }
      httpResponse
  }

  /**
    * Log the response in failure state.
    *
    * @return the current HttpResponse.
    */
  def logHttpResponseFailureAndReturnFields: PartialFunction[(String, HttpResponse, List[String]), List[String]] = {
    case (fullUri, httpResponse, l) =>
      if (httpResponse.status.isFailure) {
        logger.warn(s"Request ${fullUri} failed with status ${httpResponse.status}")
      }
      l
  }
}
