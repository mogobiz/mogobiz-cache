package com.mogobiz.cache.graph

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, _}
import com.mogobiz.cache.enrich._
import com.mogobiz.cache.exception.UnsupportedConfigException
import com.mogobiz.cache.utils.{CustomSslConfiguration, HeadersUtils, UrlUtils}
import com.typesafe.scalalogging.LazyLogging
import spray.client.pipelining._
import spray.http._
import spray.httpx.RequestBuilding

import scala.concurrent.Future

case class CacheFlow(sourceConfig: CacheConfig, outputConfig: CacheConfig, purgeConfig: CacheConfig)

case class FlowItem(protocolHostPort: String, relativeUrl: String, httpResponse: Option[HttpResponse])

object CacheGraph extends LazyLogging with RequestBuilding {

  /**
    *
    * @param cacheConfig
    * @param actorSystem
    * @param actorMaterializer
    * @return an akka stream source
    */
  def buildSource(cacheConfig: CacheConfig)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer): Source[Map[String, String], Unit] = {
    cacheConfig match {
      // Build a list of one element
      case _: NoConfig => Source.single(Map())
      // Build Es Source
      case c: EsConfig => {
        Source(c.getEsIterator().toStream)
          .map(hit => {
            c.fields.map(f => {
              (s"${c.`type`}.${f}", hit.getOrElse(f, List()))
            })
          })
          //remove all the hits that doesn't have all fields
          .filter { fieldsPathsAndValues =>
          val result: Boolean = fieldsPathsAndValues.count {
            case (fieldPath, values) => !values.isEmpty
          } == c.fields.length
          if (!result) {
            logger.warn("One hit doesn't have all required fields")
          }
          result
        }
          // currently keep the first value in case where a field has multiple value.
          .map { fieldsPathsAndValues =>
          fieldsPathsAndValues.map {
            case (fieldPath, values) => (fieldPath, values(0))
          }
        }
          // encode fields
          .map { fieldsPathsAndValues =>
          fieldsPathsAndValues.zip(c.encodeFields).map {
            case ((fieldPath, value), true) => fieldPath -> URLEncoder.encode(value, StandardCharsets.UTF_8.name())
            case ((fieldPath, value), _) => fieldPath -> value
          }.toMap
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
      case c: HttpConfig => Flow[Map[String, String]].map { (fields: Map[String, String]) => {
        val relativeUri: String = c.getRelativeUri(fields)
        val protocolHostPort: String = c.getProtocolHostPort
        logger.info(s"Building uri ${protocolHostPort}${relativeUri}")
        FlowItem(protocolHostPort, relativeUri, None)
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
  def buildHttpCallFlow(cacheConfig: CacheConfig, pipeline: Future[SendReceive], transformUri: (String) => String = (uri) => uri)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer) = {
    import actorSystem.dispatcher
    cacheConfig match {
      case c: HttpConfig => Flow[FlowItem].mapAsyncUnordered(c.maxClient) { (flowItem: FlowItem) => {
        val targetUrl = s"${flowItem.protocolHostPort}${transformUri(flowItem.relativeUrl)}"
        logger.info(s"Requesting ${c.method} ${targetUrl}")
        val request: HttpRequest = HttpRequest(c.method, targetUrl, buildHeaders(c))
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
    def globalPurgeCacheGraph(cacheFlow: CacheFlow)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer):
    RunnableGraph[Future[Unit]] = {
      import actorSystem.dispatcher
      cacheFlow match {
        case CacheFlow(s: NoConfig, h: HttpConfig, p: PurgeConfig) => {
          val pipeline = CustomSslConfiguration.getPipeline(h.host, h.port, h.protocol.toLowerCase == "https")
          val source: Source[Map[String, String], Unit] = buildSource(s)
          val requestFlow = buildRequestInfoFlow(h.copy(uri = p.uri))
          val purgeHttpConfig: HttpConfig = h.copy(method = p.method, uri = p.uri, additionalHeaders = p.additionalHeaders)
          val purgeHttpCallFlow = buildHttpCallFlow(purgeHttpConfig, pipeline)
          source.via(requestFlow).via(purgeHttpCallFlow).via(logHttpResponseFailure(p.method)).toMat(Sink.ignore)(Keep.right)
        }
        case CacheFlow(source, sink, purge) => throw UnsupportedConfigException(s"Input[${source.getClass.getName}] to Sink[${sink.getClass.getName}] not supported")
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
      case CacheFlow(s: CacheConfig, h: HttpConfig, p: PurgeConfig) => {
        val pipeline = CustomSslConfiguration.getPipeline(h.host, h.port, h.protocol.toLowerCase == "https")
        val source: Source[Map[String, String], Unit] = buildSource(s)
        val requestFlow = buildRequestInfoFlow(h)
        val transformPurgeUri = (uri: String) => {
          UrlUtils.uriAsStringContext(p.uri).s(uri)
        }
        val purgeHttpCallFlow = if (p.isByUri) {
          val purgeHttpConfig: HttpConfig = h.copy(method = p.method, uri = p.uri, additionalHeaders = p.additionalHeaders)
          buildHttpCallFlow(purgeHttpConfig, pipeline, transformPurgeUri)
        } else {
          // ignore global purge config in cache graph
          Flow[FlowItem].map(fi => fi.copy(httpResponse = None))
        }
        val cacheHttpCallFlow = buildHttpCallFlow(h, pipeline)
        source.via(requestFlow).via(purgeHttpCallFlow).via(logHttpResponseFailure(p.method, transformPurgeUri)).via(cacheHttpCallFlow).via(logHttpResponseFailure(h.method)).toMat(Sink.ignore)(Keep.right)
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
  def logHttpResponseFailure(method:HttpMethod, transformUri: (String) => String = (uri) => uri) = Flow[FlowItem].map {
    flowItem =>
      flowItem.httpResponse.filter(_.status.isFailure).foreach(h =>
        logger.warn(s"Request ${method} ${flowItem.protocolHostPort}${transformUri(flowItem.relativeUrl)} failed with status ${h.status}")
      )
      flowItem
  }
}
