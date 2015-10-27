package com.mogobiz.cache.bin

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph
import com.mogobiz.cache.enrich.ConfigHelpers._
import com.mogobiz.cache.enrich.NoConfig
import com.mogobiz.cache.graph.{CacheFlow, CacheGraph}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import spray.http.{HttpMethods, HttpMethod, HttpRequest}

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
 * Process Cache provides the ability to call different URLs defined in the application.conf file.
 * It currently support ESInput and HttpOutput only.
 */
object ProcessCache extends LazyLogging {

  /**
   * Run each runnable graph sequentially. Stop at the first failure.
   * @param runnableGraphs
   * @param system
   * @param actorMaterializer
   */
  private def run(runnableGraphs: List[RunnableGraph[Future[Unit]]])(implicit system: ActorSystem, actorMaterializer: ActorMaterializer){
    import system.dispatcher
    def run(result: Future[Unit], remainingRunnableGraphs: List[RunnableGraph[Future[Unit]]]): Unit = {
      result.onComplete {
        case Success(_) => {
          remainingRunnableGraphs match {
            case first :: rest => run(first.run(), rest)
            case _ => {
              system.shutdown()
              logger.info(s"Successfully run ${runnableGraphs.length} jobs")
            }
          }
        }
        case Failure(e) => {
          system.shutdown()
          logger.error(s"Failed at job ${runnableGraphs.length - remainingRunnableGraphs.length} of ${runnableGraphs.length}")
          logger.error(e.getMessage, e)
        }
      }
    }
    run(runnableGraphs.head.run(), runnableGraphs.tail)
  }

  /**
   *
   * @param configProcessPath
   * @param system
   * @param actorMaterializer
   * @return runnables graphs built from the configuration file.
   *         Each flow described has an optional Input and an Output.
   *         Currently, Input is an ES input and Output is a Http output.
   *         If no input is found, a NoConfig is created else an EsConfig.
   *         An output is of type HttpConfig
   */
  private def buildRunnablesGraphs(configProcessPath: String)(implicit system: ActorSystem, actorMaterializer: ActorMaterializer): List[RunnableGraph[Future[Unit]]] = {
    implicit val config: Config = ConfigFactory.load()
    val runnablesGraphs: List[RunnableGraph[Future[Unit]]] = config.getConfigList(configProcessPath).filter(aConfig => aConfig.hasPath("input") || aConfig.hasPath("output")).map(aConfig => {
      val source = if (aConfig.hasPath("input")) aConfig.getConfig("input").toEsConfig else NoConfig()
      val sink = aConfig.getConfig("output").toHttpConfig()
      CacheFlow(source, sink)
    }).map(CacheGraph.cacheRunnableGraph).toList
    logger.info(s"Built ${runnablesGraphs.length} runnables graphs from ${configProcessPath}")
    runnablesGraphs
  }

  def registerSprayHttpMethods(): Unit ={
    HttpMethods.register(HttpMethod.custom("PURGE", true, true, false))
    HttpMethods.register(HttpMethod.custom("BAN", true, true, false))
  }

  /**
   * Run the different jobs described in the configuration file.
   *
   * @param args need the config process path defined in an application.conf { @see src/samples/conf/}
   */
  def main(args: Array[String]) {
    if (args.length != 1) {
      logger.error(s"USAGE : ProcessCache <process path>")
      System.exit(-1)
    } else {
      implicit val system = ActorSystem()
      implicit val _ = ActorMaterializer()
      registerSprayHttpMethods()
      try {
        run(buildRunnablesGraphs(args(0)))
      } catch {
        case e: Throwable => {
          logger.error(e.getMessage, e)
          system.shutdown()
        }
      }
    }
  }
}
