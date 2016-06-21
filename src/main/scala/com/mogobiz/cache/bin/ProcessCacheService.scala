package com.mogobiz.cache.bin

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

  val genericProcessCache ="mogobiz.cache.uri.generic.process"
  val genericPurge ="mogobiz.cache.uri.generic.purge"
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
   * @param configs
   * @param system
   * @param actorMaterializer
   * @return runnables graphs built from the configuration file.
   *         Each flow described has an optional Input and an Output.
   *         Currently, Input is an ES input and Output is a Http output.
   *         If no input is found, a NoConfig is created else an EsConfig.
   *         An output is of type HttpConfig
   */
  private def buildRunnablesGraphs(configs :List[Config], purgeConfig:Config)(implicit system: ActorSystem, actorMaterializer: ActorMaterializer): List[RunnableGraph[Future[Unit]]] = {
    val purgeHttpConfig: HttpConfig = purgeConfig.toHttpConfig()
    def buildRunnableGraphs(config:Config)={
      implicit val configImpl = config
      config.getConfigList(genericProcessCache).filter(aConfig => aConfig.hasPath("input") || aConfig.hasPath("output")).map(aConfig => {
        val source = if (aConfig.hasPath("input")) aConfig.getConfig("input").toEsConfig else NoConfig()
        val sink = aConfig.getConfig("output").toHttpConfig()
        CacheFlow(source, sink, purgeHttpConfig)
      }).map(CacheGraph.cacheRunnableGraph).toList
    }
    val runnablesGraphs: List[RunnableGraph[Future[Unit]]] = configs.flatMap(c => buildRunnableGraphs(c))
    logger.info(s"Built ${runnablesGraphs.length} runnables graphs")
    if(purgeConfig.getString("uri") == "{0}")
      runnablesGraphs
    else
      CacheGraph.cacheRunnableGraph(CacheFlow(NoConfig(),NoConfig(),purgeHttpConfig)) :: runnablesGraphs
  }

  private def registerSprayHttpMethods(): Unit ={
    HttpMethods.getForKey("PURGE") match {
      case None => HttpMethods.register(HttpMethod.custom("PURGE", true, true, false))
      case _ =>
    }
    HttpMethods.getForKey("BAN") match {
      case None => HttpMethods.register(HttpMethod.custom("BAN", true, true, false))
      case _ =>
    }
  }

  private def buildStoreAndPrefixConfig(apiPrefix:String, apiStore:String, frontPrefix:String, frontStore:String) = {
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

  private def buildStaticUrlsConfig(staticUrls:List[String]) = {
    val staticUrlsConfigAsString = staticUrls.map(l => {
      s"""{output: {
        uri: ${l}
        server: $${mogobiz.cache.server.api}
      }}""".stripMargin
    }).mkString("mogobiz.cache.uri.generic.process:[",",","]")
    ConfigFactory.parseString(staticUrlsConfigAsString)
  }

  def run(apiPrefix:String, apiStore:String, frontPrefix:String, frontStore:String, staticUrls:List[String]){
    logger.info("Starting to cache data")
    implicit val system = ActorSystem()
    implicit val _ = ActorMaterializer()
    registerSprayHttpMethods()
    try {
      val storeAndPrefixConfig: Config = buildStoreAndPrefixConfig(apiPrefix, apiStore, frontPrefix, frontStore)
      val genericConfig = storeAndPrefixConfig.withFallback(ConfigFactory.parseResources("application.conf")).resolve()
      val staticUrlsConfig = storeAndPrefixConfig.withFallback(buildStaticUrlsConfig(staticUrls)).withFallback(ConfigFactory.parseResources("application.conf")).resolve()
      val purgeConfig = ConfigFactory.load().getConfig(genericPurge)
      run(buildRunnablesGraphs(List(genericConfig, staticUrlsConfig), purgeConfig))
    } catch {
      case e: Throwable => {
        logger.error(e.getMessage, e)
        system.shutdown()
      }
    }
  }
}
