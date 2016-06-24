package com.mogobiz.cache.service

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph
import com.mogobiz.cache.enrich.ConfigHelpers._
import com.mogobiz.cache.enrich.{HttpConfig, NoConfig}
import com.mogobiz.cache.graph.{CacheFlow, CacheGraph}
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import spray.http.{HttpMethod, HttpMethods}

import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Process Cache provides the ability to call different URLs defined in the application.conf file.
  * It currently support ESInput and HttpOutput only.
  */
object ProcessCacheService extends LazyLogging {

  val genericProcessCache = "mogobiz.cache.uri.generic.process"
  val genericPurge = "mogobiz.cache.uri.generic.purge"

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
    val purgeHttpConfig: HttpConfig = config.getConfig(genericPurge).toHttpConfig()
    def buildRunnableGraphs(config: Config) = {
      implicit val configImpl = config
      config.getConfigList(genericProcessCache).filter(aConfig => aConfig.hasPath("input") || aConfig.hasPath("output")).map(aConfig => {
        val source = if (aConfig.hasPath("input")) aConfig.getConfig("input").toEsConfig else NoConfig()
        val sink = aConfig.getConfig("output").toHttpConfig()
        CacheFlow(source, sink, purgeHttpConfig)
      }).map(CacheGraph.cacheRunnableGraph).toList
    }
    val runnablesGraphs: List[RunnableGraph[Future[Unit]]] = buildRunnableGraphs(config)
    logger.info(s"Built ${runnablesGraphs.length} runnables graphs")
    if (purgeHttpConfig.uri == "{0}")
      runnablesGraphs
    else
      CacheGraph.cacheRunnableGraph(CacheFlow(NoConfig(), NoConfig(), purgeHttpConfig)) :: runnablesGraphs
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
    * @param apiPrefix   prefix for the API
    * @param apiStore    name of the API store
    * @param frontPrefix prefix for the frontend
    * @param frontStore  name of the front store
    * @return A config not resolved that contains all informations related with the parameters
    */
  private def buildStoreAndPrefixConfig(apiPrefix: String, apiStore: String, frontPrefix: String, frontStore: String) = {
    val configAsString: String =
      s"""
         |mogobiz.cache.uri.generic{
         | apiStore: "${apiStore}"
         | frontStore: "${frontStore}"
         | apiPrefix: "${apiPrefix}"
         | frontPrefix: "${frontPrefix}"
         |}
    """.stripMargin
    ConfigFactory.parseString(
      configAsString)
  }

  /**
    *
    * @param staticUrls list of static urls which can benefit with substitutions from the whole config
    * @return a config not resolved
    */
  private def buildStaticUrlsConfig(staticUrls: List[String]): Config = {
    val sanitizedStaticUrls = staticUrls.filter(s => !s.isEmpty)
    if(sanitizedStaticUrls.isEmpty) {
      ConfigFactory.empty()
    }
    else {
      sanitizedStaticUrls.map(l => {
        s"""mogobiz.cache.uri.generic.process += {output: {
        uri: ${l}
        server: $${mogobiz.cache.server.api}
      }}""".stripMargin
      }).map(s => ConfigFactory.parseString(s))
        .reduce((c1, c2) => c2.withFallback(c1))
    }
  }

  /**
    *
    * @param apiPrefix   prefix for the API
    * @param apiStore    name of the API store
    * @param frontPrefix prefix for the frontend
    * @param frontStore  name of the front store
    * @param staticUrls list of static urls which can benefit with substitutions from the whole config. Each URL are separated by ":" character
    * @return The whole config resolved
    */
  private def buildWholeConfig(apiPrefix: String, apiStore: String, frontPrefix: String, frontStore: String, staticUrls: List[String]) = {
    buildStoreAndPrefixConfig(apiPrefix, apiStore, frontPrefix, frontStore)
      .withFallback(buildStaticUrlsConfig(staticUrls))
      .withFallback(ConfigFactory.parseResources("application.conf"))
      .resolve()
  }

  /**
    *
    * @param apiPrefix   prefix for the API
    * @param apiStore    name of the API store
    * @param frontPrefix prefix for the frontend
    * @param frontStore  name of the front store
    * @param staticUrls  list of static urls which can benefit with substitutions from the whole config. Each URL are separated by ":" character
    */
  def run(apiPrefix: String, apiStore: String, frontPrefix: String, frontStore: String, staticUrls: List[String]) {
    logger.info("Starting to cache data")
    implicit val system = ActorSystem()
    implicit val _ = ActorMaterializer()
    registerSprayHttpMethods()
    try {
      val config = buildWholeConfig(apiPrefix, apiStore, frontPrefix, frontStore, staticUrls)
      run(buildRunnablesGraphs(config))
    } catch {
      case e: Throwable => {
        logger.error(e.getMessage, e)
        system.shutdown()
      }
    }
  }
}
