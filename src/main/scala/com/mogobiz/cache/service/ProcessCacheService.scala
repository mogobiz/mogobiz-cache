package com.mogobiz.cache.service

import akka.actor.ActorSystem
import akka.stream.scaladsl.RunnableGraph
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.mogobiz.cache.enrich.ConfigHelpers._
import com.mogobiz.cache.enrich.{HttpConfig, NoConfig, PurgeConfig}
import com.mogobiz.cache.graph.{CacheFlow, CacheGraph}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject}
import com.typesafe.scalalogging.LazyLogging
import spray.http.{HttpMethod, HttpMethods, IllegalUriException}

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
      result.onComplete {
        case Success(_) => {
          remainingRunnableGraphs match {
            case first :: rest => run(first.run(), rest)
            case _ => {
              system.shutdown()
              logger.info(s"Successfully run ${runnableGraphs.length} jobs")
              if(errorEncountered){
                logger.info("Still, some errors have been encountered. Please check logs to have more details.")
              }
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
    * @param config
    * @param system
    * @param actorMaterializer
    * @return runnables graphs built from the configuration file.
    *         Each flow described has an optional Input and an Output.
    *         Currently, Input is an ES input and Output is a Http output.
    *         If no input is found, a NoConfig is created else an EsConfig.
    *         An output is of type HttpConfig
    */
  private def buildRunnablesGraphs(config: Config)(implicit system: ActorSystem, actorMaterializer: ActorMaterializer): List[RunnableGraph[Future[Unit]]] = {
    val purgeHttpConfig: Map[String, PurgeConfig] = config.getObject(genericPurge).entrySet().map(entry => {
      val purgeConfig: Config = entry.getValue.asInstanceOf[ConfigObject].toConfig
      entry.getKey -> purgeConfig.toPurgeConfig()
    }).toMap
    def buildCacheFlows(config: Config) = {
      implicit val configImpl = config
      config.getConfigList(genericProcessCache).filter(aConfig => aConfig.hasPath("input") || aConfig.hasPath("output")).map(aConfig => {
        val source = if (aConfig.hasPath("input")) aConfig.getConfig("input").toEsConfig else NoConfig()
        val sink = aConfig.getConfig("output").toHttpConfig()
        CacheFlow(source, sink, purgeHttpConfig.getOrElse(sink.host, purgeHttpConfig.get("generic").get))
      }).toList
    }
    def buildRunnableGraphs(cacheFlows: List[CacheFlow]) = {
      cacheFlows.map(CacheGraph.cacheRunnableGraph)
    }

    val cacheFlows: List[CacheFlow] = buildCacheFlows(config)
    val globalPurge = cacheFlows.filterNot{
      case CacheFlow(_, _, purgeConfig:PurgeConfig) => purgeConfig.isByUri
    }
    val runnablesGraphs: List[RunnableGraph[Future[Unit]]] = buildRunnableGraphs(cacheFlows)
    logger.info(s"Built ${runnablesGraphs.length} runnables graphs")
    val globalPurgeGraphs: List[RunnableGraph[Future[Unit]]] = globalPurge.map{
      case CacheFlow(_, httpConfig:HttpConfig, purgeConfig:PurgeConfig) => CacheGraph.globalPurgeCacheGraph(CacheFlow(NoConfig(), httpConfig, purgeConfig))
    }
    globalPurgeGraphs ::: runnablesGraphs
  }

  /**
    * Register additionnal spray Http methods
    */
  private def registerSprayHttpMethods(): Unit = {
    HttpMethods.getForKey("PURGE") match {
      case None => HttpMethods.register(HttpMethod.custom("PURGE", true, true, false))
      case _ =>
    }
    HttpMethods.getForKey("BAN") match {
      case None => HttpMethods.register(HttpMethod.custom("BAN", true, true, false))
      case _ =>
    }
  }

  /**
    *
    * @param apiStore    name of the API store
    * @param staticUrls  list of static urls which can benefit with substitutions from the whole config. Each URL are separated by ":" character
    * @return The whole config resolved
    */
  private def buildWholeConfig(apiStore: String, staticUrls: List[String]) = {
    buildStaticUrlsConfig(apiStore, staticUrls)
      .withFallback(ConfigFactory.parseResources("application.conf"))
      .resolve()
  }

  /**
    *
    * @param apiStore    name of the API store
    * @param staticUrls  list of static urls which can benefit with substitutions from the whole config. Each URL are separated by ":" character
    */
  def run(apiStore: String, staticUrls: List[String]) {
    logger.info("Starting to cache data")
    val decider:Supervision.Decider = {
      case e:IllegalUriException => {
        logger.error(e.getMessage)
        errorEncountered = true
        Supervision.resume
      }
      case _ => Supervision.stop
    }
    implicit val system = ActorSystem()
    implicit val _ = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))
    registerSprayHttpMethods()
    try {
      val config = buildWholeConfig(apiStore, staticUrls)
      run(buildRunnablesGraphs(config))
    } catch {
      case e: Throwable => {
        logger.error(e.getMessage, e)
        system.shutdown()
      }
    }
  }
}
