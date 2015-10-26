package com.mogobiz.cache.graph

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import com.mogobiz.cache.bin.CacheFlow
import com.mogobiz.cache.enrich.{EsConfig, HttpConfig, NoConfig}
import com.mogobiz.cache.exception.UnsupportedConfigException
import com.typesafe.config.Config
import spray.client.pipelining._

import scala.concurrent.Future

object CacheGraph {

  def cacheRunnableGraph(cacheFlow:CacheFlow)(implicit rootConfig:Config, actorSystem:ActorSystem, actorMaterializer:ActorMaterializer):
  RunnableGraph[Future[Unit]] = {
    import actorSystem.dispatcher
    cacheFlow match {
      case CacheFlow(_: NoConfig, h: HttpConfig) => Source.single(1).mapAsyncUnordered(h.maxClient) { i =>
        val pipeline: SendReceive = sendReceive
        println(s"Calling ${h.getFullUri()}")
        pipeline(Get(h.getFullUri()))
      }.toMat(Sink.ignore)(Keep.right)
      case CacheFlow(c: EsConfig,h:HttpConfig) => Source(c.getEsIterator().toStream).map { hit => {
        c.fields.map(hit.getOrElse(_,List()))
      }}.filter(_.filter(!_.isEmpty).length == c.fields.length).map(_(0)).mapAsyncUnordered(h.maxClient)
      {fields =>{
        val pipeline: SendReceive = sendReceive
        println(s"Calling ${h.getFullUri(fields)}")
        pipeline(Get(h.getFullUri(fields)))
      }}.toMat(Sink.ignore)(Keep.right)
      case c => throw UnsupportedConfigException(c.getClass.getName + " not supported")
    }
  }
}
