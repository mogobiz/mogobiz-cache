package com.mogobiz.cache.utils

import java.security.cert.X509Certificate
import java.util
import java.util.concurrent.TimeUnit
import javax.net.ssl._

import com.mogobiz.cache.enrich.ConfigHelpers._
import com.typesafe.config.ConfigFactory
import okhttp3.OkHttpClient.Builder
import okhttp3._

object CustomSslOkHttpClient {
  private val hv = new HostnameVerifier() {
    override def verify(s: String, sslSession: SSLSession): Boolean = true
  }

  object BlindFaithX509TrustManager extends X509TrustManager {
    def checkClientTrusted(chain: Array[X509Certificate], authType: String) = ()

    def checkServerTrusted(chain: Array[X509Certificate], authType: String) = ()

    def getAcceptedIssuers = Array[X509Certificate]()
  }

  private val sslSocketFactory: SSLSocketFactory = {
    val context = SSLContext.getInstance("TLS")
    context.init(Array[KeyManager](), Array(BlindFaithX509TrustManager), null)
    context.getSocketFactory
  }

  private val httpsConnectionSpec = new ConnectionSpec.Builder(ConnectionSpec.MODERN_TLS)
    .tlsVersions(TlsVersion.TLS_1_2)
    .cipherSuites(
      CipherSuite.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
      CipherSuite.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
      CipherSuite.TLS_DHE_RSA_WITH_AES_128_GCM_SHA256)
    .build()

  private val clearTextConnectionSpec = new ConnectionSpec.Builder(ConnectionSpec.CLEARTEXT).build()


  def getClient(okAuthenticator: Option[Authenticator] = None): OkHttpClient = {
    val clientBuilder: Builder = new OkHttpClient.Builder()
      .connectionSpecs(util.Arrays.asList(httpsConnectionSpec, clearTextConnectionSpec))
      .sslSocketFactory(sslSocketFactory, BlindFaithX509TrustManager)
      .hostnameVerifier(hv)
      .connectTimeout(ConfigFactory.load().getOrElse("mogobiz.cache.timeout", 30).toLong, TimeUnit.SECONDS)
    okAuthenticator match {
      case Some(authenticator) => clientBuilder.authenticator(authenticator).build()
      case _ => clientBuilder.build()
    }
  }
}
