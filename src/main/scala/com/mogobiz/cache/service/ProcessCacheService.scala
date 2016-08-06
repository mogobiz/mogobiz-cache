package com.mogobiz.cache.service

import akka.actor.ActorSystem
import akka.stream.scaladsl.RunnableGraph
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.mogobiz.cache.enrich.ConfigHelpers._
import com.mogobiz.cache.enrich.{ConfigHelpers, HttpConfig, PurgeConfig}
import com.mogobiz.cache.graph.{CacheFlow, CacheGraph}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Process Cache provides the ability to call different URLs defined in the application.conf file.
  * It currently support ESInput and HttpOutput only.
  */
object ProcessCacheService extends LazyLogging {

  var errorEncountered = false

  val genericProcessCache = "mogobiz.cache.uri.generic.process"
  val genericPurge = "mogobiz.cache.purges"

  /**
    * Run each runnable graph sequentially. Stop at the first failure.
    *
    * @param runnableGraphs
    * @param system
    * @param actorMaterializer
    */
  private def run(runnableGraphs: List[RunnableGraph[Future[Unit]]])(implicit system: ActorSystem, actorMaterializer: ActorMaterializer) {
    import system.dispatcher
    def run(result: Future[Unit], remainingRunnableGraphs: List[RunnableGraph[Future[Unit]]]): Unit = {
      logger.info(s"Starting job ${runnableGraphs.length - remainingRunnableGraphs.length} out of ${runnableGraphs.length}")
      def runNextJob = remainingRunnableGraphs match {
        case first :: rest => run(first.run(), rest)
        case _ => {
          system.shutdown()
          logger.info(s"Successfully run ${runnableGraphs.length} jobs")
          if(errorEncountered){
            logger.info("Still, some errors have been encountered. Please check logs to have more details.")
          }
        }
      }
      result.onComplete {
        case Success(_) => {
          logger.info(s"Successfully ran job ${runnableGraphs.length - remainingRunnableGraphs.length} out of ${runnableGraphs.length}")
          runNextJob
        }
        case Failure(e) => {
          logger.error(s"Failed at job ${runnableGraphs.length - remainingRunnableGraphs.length} out of ${runnableGraphs.length}")
          logger.error(e.getMessage, e)
          runNextJob
        }
      }
    }
    run(runnableGraphs.head.run(), runnableGraphs.tail)
  }

  /**
    *
    * @param cacheFlows
    * @param system
    * @param actorMaterializer
    * @return runnables graphs built from the configuration file.
    *         Each flow described has an optional Input and an Output.
    *         Currently, Input is an ES input and Output is a Http output.
    *         If no input is found, a NoConfig is created else an EsConfig.
    *         An output is of type HttpConfig
    */
  private def buildRunnablesGraphs(cacheFlows: List[CacheFlow])(implicit system: ActorSystem, actorMaterializer: ActorMaterializer): List[RunnableGraph[Future[Unit]]] = {
    val config: Config = ConfigFactory.load()
    val purgeHttpConfig: Map[String, PurgeConfig] = config.getObject(genericPurge).entrySet().map(entry => {
      val purgeConfig: Config = entry.getValue.asInstanceOf[ConfigObject].toConfig
      entry.getKey -> purgeConfig.toPurgeConfig()
    }).toMap
    def buildRunnableGraphs(cacheFlows: List[CacheFlow]) = {
      cacheFlows.map(CacheGraph.cacheRunnableGraph)
    }

    val cacheFlowsWithPurge: List[CacheFlow] = cacheFlows.map(cacheFlow => {
      cacheFlow.copy(purgeConfig = purgeHttpConfig.getOrElse(cacheFlow.outputConfig.host, purgeHttpConfig.get("generic").get))
    })
    val globalPurge = cacheFlowsWithPurge.filterNot{
      case CacheFlow(_, _, purgeConfig:PurgeConfig) => purgeConfig.isByUri
    }
    val runnablesGraphs: List[RunnableGraph[Future[Unit]]] = buildRunnableGraphs(cacheFlowsWithPurge)
    logger.info(s"Built ${runnablesGraphs.length} runnables graphs")
    val globalPurgeGraphs: List[RunnableGraph[Future[Unit]]] = globalPurge.map{
      case CacheFlow(_, httpConfig:HttpConfig, purgeConfig:PurgeConfig) => CacheGraph.globalPurgeCacheGraph(CacheFlow(None, httpConfig, purgeConfig))
    }
    globalPurgeGraphs ::: runnablesGraphs
  }

  /**
    *
    * @param apiStore    name of the API store
    * @param staticUrls  list of static urls which can benefit with substitutions from the whole config. Each URL are separated by ":" character
    */
  def run(apiStore: String, staticUrls: List[String]) {
    logger.info("Starting to cache data")
    val decider:Supervision.Decider = {
//      case e:IllegalUriException => {
//        logger.error(e.getMessage)
//        errorEncountered = true
//        Supervision.resume
//      }
      case _ => Supervision.stop
    }
    implicit val system = ActorSystem()
    implicit val _ = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
    try {
      val config: List[CacheFlow] = ConfigHelpers.buildStaticUrlsConfig(apiStore, staticUrls)
      run(buildRunnablesGraphs(config))
    } catch {
      case e: Throwable => {
        logger.error(e.getMessage, e)
        system.shutdown()
      }
    }
  }
}
