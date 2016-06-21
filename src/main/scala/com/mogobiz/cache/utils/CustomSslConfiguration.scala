package com.mogobiz.cache.utils

import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit
import javax.net.ssl.{HttpsURLConnection, _}

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import spray.can.Http
import spray.client.pipelining._
import spray.io.ClientSSLEngineProvider

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by boun on 14/06/2016.
  */
object CustomSslConfiguration {
  val hv = new HostnameVerifier() {
    override def verify(s: String, sslSession: SSLSession): Boolean = true
  }

  private implicit val trustfulSslContext: SSLContext = {
    object BlindFaithX509TrustManager extends X509TrustManager {
      def checkClientTrusted(chain: Array[X509Certificate], authType: String) = ()

      def checkServerTrusted(chain: Array[X509Certificate], authType: String) = ()

      def getAcceptedIssuers = Array[X509Certificate]()
    }

    val context = SSLContext.getInstance("TLS")
    context.init(Array[KeyManager](), Array(BlindFaithX509TrustManager), null)
    HttpsURLConnection.setDefaultHostnameVerifier(hv)
    HttpsURLConnection.setDefaultSSLSocketFactory(context.getSocketFactory)
    context
  }

  private implicit val sslEngineProvider: ClientSSLEngineProvider = {
    // To enable TLS 1.2 you can use the following setting of the VM: -Dhttps.protocols=TLSv1.1,TLSv1.2
    ClientSSLEngineProvider { engine =>
      engine.setEnabledProtocols(Array("TLSv1.2", "TLSv1.1", "SSLv3"))
      engine
    }
  }

  private implicit val timeout = {
    import com.mogobiz.cache.enrich.ConfigHelpers._
    Timeout(ConfigFactory.load().getOrElse("mogobiz.cache.timeout", 30).toLong, TimeUnit.SECONDS)
  }

  def getPipeline(host:String, port:Integer, ssl:Boolean)(implicit actorSystem:ActorSystem, executionContext: ExecutionContext): Future[SendReceive] = {
    for (
      Http.HostConnectorInfo(connector, _) <- IO(Http) ? Http.HostConnectorSetup(host, port, sslEncryption = ssl)(actorSystem, sslEngineProvider)
    ) yield sendReceive(connector)

  }
}
