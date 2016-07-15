package com.mogobiz.cache.stage

import akka.actor.ActorSystem
import akka.stream.stage.{Context, PushPullStage, SyncDirective, TerminationDirective}
import com.mogobiz.cache.enrich.EsConfig
import com.typesafe.scalalogging.LazyLogging

/**
  * Created by boun on 15/07/2016.
  */
class EsCombinator(esConfig: EsConfig)(implicit actorSystem: ActorSystem) extends PushPullStage[Map[String, List[String]], Map[String, List[String]]] with LazyLogging {

  private var iterator: Iterator[Map[String, List[String]]] = esConfig.getEsIterator()
  //this flag tells that the iterator will always be empty
  private val emptyIterator = {
    if (!iterator.hasNext) {
      logger.warn(s"${esConfig.index} ${esConfig.`type`} doesn't have any entries")
    }
    !iterator.hasNext
  }

  private var lastElement: Map[String, List[String]] = _

  override def onPull(ctx: Context[Map[String, List[String]]]): SyncDirective = {
    if (ctx.isFinishing) {
      logger.error("is finishing")
      if(lastElement == null || !iterator.hasNext){
        ctx.finish()
      } else {
        ctx.push(lastElement ++ iterator.next())
      }
    } else {
      if (emptyIterator) {
        ctx.pull
      } else if(lastElement == null){
        ctx.pull()
      } else if (iterator.hasNext) {
        ctx.push(lastElement ++ iterator.next())
      } else {
        ctx.pull()
      }
    }
  }

  override def onPush(elem: Map[String, List[String]], ctx: Context[Map[String, List[String]]]): SyncDirective = {
    if (emptyIterator) {
      ctx.push(elem)
    } else {
      lastElement = elem
      if (!iterator.hasNext) {
        iterator = esConfig.getEsIterator()
      }
      ctx.push(lastElement ++ iterator.next())
    }
  }

  override def onUpstreamFinish(ctx: Context[Map[String, List[String]]]): TerminationDirective =
    ctx.absorbTermination()
}
