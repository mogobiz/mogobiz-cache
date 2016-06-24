package com.mogobiz.cache.graph

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, _}
import com.mogobiz.cache.enrich.{CacheConfig, EsConfig, HttpConfig, NoConfig}
import com.mogobiz.cache.exception.UnsupportedConfigException
import com.mogobiz.cache.utils.{CustomSslConfiguration, HeadersUtils}
import com.typesafe.scalalogging.LazyLogging
import spray.client.pipelining._
import spray.http._
import spray.httpx.RequestBuilding

import scala.concurrent.Future

case class CacheFlow(sourceConfig: CacheConfig, outputConfig: CacheConfig, purgeConfig: CacheConfig)

case class FlowItem(fullUri: String, httpResponse: Option[HttpResponse])

object CacheGraph extends LazyLogging with RequestBuilding {

  /**
    *
    * @param cacheConfig
    * @param actorSystem
    * @param actorMaterializer
    * @return an akka stream source
    */
  def buildSource(cacheConfig: CacheConfig)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer): Source[List[String], Unit] = {
    cacheConfig match {
      // Build a list of one element
      case _: NoConfig => Source.single(List())
      // Build Es Source
      case c: EsConfig => {
        Source(c.getEsIterator().toStream)
          .map(hit => {
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
      }
    }
  }

  /**
    *
    * @param cacheConfig
    * @param actorSystem
    * @param actorMaterializer
    * @return a flow which returns the request
    */
  def buildRequestInfoFlow(cacheConfig: CacheConfig)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer) = {
    cacheConfig match {
      case c: HttpConfig => Flow[List[String]].map { (fields: List[String]) => {
        val fullUri: String = c.getFullUri(fields)
        logger.info(s"Building uri ${fullUri}")
        FlowItem(fullUri, None)
      }
      }
    }
  }

  /**
    *
    * @param cacheConfig
    * @param pipeline
    * @param actorSystem
    * @param actorMaterializer
    * @return a flow which execute an HttpCall if possible
    */
  def buildHttpCallFlow(cacheConfig: CacheConfig, pipeline: Future[SendReceive])(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer) = {
    import actorSystem.dispatcher
    cacheConfig match {
      case c: HttpConfig => Flow[FlowItem].mapAsyncUnordered(c.maxClient) { (flowItem: FlowItem) => {
        logger.info(s"Requesting ${c.method} ${flowItem.fullUri}")
        val request: HttpRequest = HttpRequest(c.method, flowItem.fullUri, buildHeaders(c))
        pipeline.flatMap(p => p(request)).map(httpResponse => flowItem.copy(httpResponse = Some(httpResponse)))
      }
      }
      case _: NoConfig => Flow[FlowItem].map(fi => fi.copy(httpResponse = None))
    }
  }

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
      case CacheFlow(s: CacheConfig, h: CacheConfig, p: HttpConfig) => {
        val pipeline = CustomSslConfiguration.getPipeline(p.host, p.port, p.protocol.toLowerCase == "https")
        val source: Source[List[String], Unit] = buildSource(s)
        val requestFlow = buildRequestInfoFlow(h)
        val purgeHttpCallFlow = if (p.uri == "{0}") buildHttpCallFlow(p, pipeline) else Flow[FlowItem].map(fi => fi.copy(httpResponse = None))
        val cacheHttpCallFlow = buildHttpCallFlow(h, pipeline)
        source.via(requestFlow).via(purgeHttpCallFlow).map(logHttpResponseFailure).via(cacheHttpCallFlow).map(logHttpResponseFailure).toMat(Sink.ignore)(Keep.right)
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
  def logHttpResponseFailure = (flowItem: FlowItem) => {
    flowItem.httpResponse.filter(_.status.isFailure).foreach(h =>
      logger.warn(s"Request ${flowItem.fullUri} failed with status ${h.status}")
    )
    flowItem
  }
}
