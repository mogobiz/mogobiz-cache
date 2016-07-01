package com.mogobiz.cache.graph

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Source, _}
import com.mogobiz.cache.enrich._
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
            val result: Boolean = fieldsPathsAndValues.count{
              case (fieldPath, values) => !values.isEmpty
            } == c.fields.length
            if (!result) {
              logger.warn("One hit doesn't have all required fields")
            }
            result
          }
          // currently keep the first value in case where a field has multiple value.
          .map { fieldsPathsAndValues =>
            fieldsPathsAndValues.map{
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

//  private def escapeVariablesForConfig(url: String): String = {
//    val wrappAllVariables: String = url.replaceAll("(\\Q${\\E.*?\\Q}\\E)", "\"$1\"")
//    val fixHead = "^\\Q${\\E.*?\\Q}\\E".r.findFirstIn(url) match {
//      case Some(_) => wrappAllVariables.tail
//      case _ => "\"" + wrappAllVariables
//    }
//    "\\Q${\\E.*?\\Q}\\E$".r.findFirstIn(url) match {
//      case Some(_) => fixHead.reverse.tail.reverse
//      case _ => fixHead + "\""
//    }
//  }

  /**
    *
    * @param cacheConfig
    * @param actorSystem
    * @param actorMaterializer
    * @return a flow which returns the request
    */
  def buildRequestInfoFlow(cacheConfig: CacheConfig)(implicit actorSystem: ActorSystem, actorMaterializer: ActorMaterializer) = {
    cacheConfig match {
      case c: HttpConfig => Flow[Map[String,String]].map { (fields: Map[String,String]) => {
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
      case CacheFlow(s: CacheConfig, h: HttpConfig, p: PurgeConfig) => {
        val pipeline = CustomSslConfiguration.getPipeline(h.host, h.port, h.protocol.toLowerCase == "https")
        val source: Source[Map[String,String], Unit] = buildSource(s)
        val requestFlow = buildRequestInfoFlow(h)
        val purgeHttpCallFlow = if (p.uri == "{0}") {
          val purgeHttpConfig: HttpConfig = h.copy(method = p.method, uri = p.uri, additionalHeaders = p.additionalHeaders)
          buildHttpCallFlow(purgeHttpConfig, pipeline)
        } else Flow[FlowItem].map(fi => fi.copy(httpResponse = None))
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
