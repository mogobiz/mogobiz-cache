package com.mogobiz.cache.bin

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph
import com.mogobiz.cache.enrich.ConfigHelpers._
import com.mogobiz.cache.enrich.{CacheConfig, NoConfig}
import com.mogobiz.cache.graph.CacheGraph
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.{Failure, Success}

case class CacheFlow(source: CacheConfig, sink: CacheConfig)

abstract class AbstractCache {

  private def run(rg: List[RunnableGraph[Future[Unit]]])(implicit system: ActorSystem, actorMaterializer: ActorMaterializer) = {
    import system.dispatcher
    def run(result: Future[Unit], rg: List[RunnableGraph[Future[Unit]]]): Unit = {
      result.onComplete {
        case Success(_) => {
          rg match {
            case first :: rest => run(first.run(), rest)
            case _ => {
              system.shutdown()
              println("finished")
            }
          }
        }
        case Failure(e) => {
          system.shutdown()
          throw e;
        }
      }
    }
    run(rg.head.run(), rg.tail)
  }

  def run(configProcessPath:String): Unit ={
    implicit val system = ActorSystem()
    implicit val _ = ActorMaterializer()
    implicit val config: Config = ConfigFactory.load()
    try {
      val allJobs: List[RunnableGraph[Future[Unit]]] = config.getConfigList(configProcessPath).filter(aConfig => aConfig.hasPath("input") || aConfig.hasPath("output")).map { aConfig => {
        val source = if (aConfig.hasPath("input")) aConfig.getConfig("input").toEsConfig else NoConfig()
        val sink = aConfig.getConfig("output").toApiHttpConfig()
        CacheFlow(source, sink)
      }}.map(CacheGraph.cacheRunnableGraph).toList
      run(allJobs)
    } catch {
      case e: Throwable => {
        system.shutdown()
        throw e
      }
    }
  }
}
