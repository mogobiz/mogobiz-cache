package com.mogobiz.cache.service

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FunSpec, PrivateMethodTester}

class ProcessCacheServiceSpec extends BuildStaticUrlsConfigSpec with BuildStoreAndPrefixConfig

trait BuildStaticUrlsConfigSpec extends FunSpec with PrivateMethodTester {
  describe("buildStaticUrlsConfig") {
    describe("with an empty list of static url") {
      it("should produce an empty Config") {
        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig(List())
        assert(config === ConfigFactory.empty())
      }
    }

    describe("with a list with empty static url") {
      it("should produce an empty Config") {
        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig(List("", ""))
        assert(config === ConfigFactory.empty())
      }
    }

    describe("with a list of one static url") {
      it("should append an element to the list mogobiz.cache.uri.generic.process") {
        val url = "customUrl"
        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig(List(url))
        val configResolved: Config = config.withFallback(ConfigFactory.parseString(
          """
            mogobiz.cache.uri.generic.process:[]
            mogobiz.cache.server.api: api
          """)).resolve()
        assert(configResolved.hasPath("mogobiz.cache.uri.generic.process"), "Doesn't contain the key: mogobiz.cache.uri.generic.process")
        val configList = configResolved.getObjectList("mogobiz.cache.uri.generic.process")
        assert(configList.size() === 1, "The list mogobiz.cache.uri.generic.process should have one element")
        val httpConfig: Config = configList.get(0).toConfig
        checkHttpConfig(httpConfig, "api", url)
      }
    }

    describe("with a list of two static url") {
      it("should append two element to the list mogobiz.cache.uri.generic.process in the same order as they have been declared") {
        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
        val url1 = "url1"
        val url2 = "url2"
        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig(List(url1, url2))
        val configResolved: Config = config.withFallback(ConfigFactory.parseString(
          """
            mogobiz.cache.uri.generic.process:[]
            mogobiz.cache.server.api: api
          """)).resolve()
        assert(configResolved.hasPath("mogobiz.cache.uri.generic.process"), "Doesn't contain the key: mogobiz.cache.uri.generic.process")
        val configList = configResolved.getObjectList("mogobiz.cache.uri.generic.process")
        assert(configList.size() === 2, "The list mogobiz.cache.uri.generic.process should have one element")
        checkHttpConfig(configList.get(0).toConfig, "api", url1)
        checkHttpConfig(configList.get(1).toConfig, "api", url2)
      }
    }
  }

  def checkHttpConfig(httpConfig: Config, server: String, uri: String): Unit = {
    assert(httpConfig.getString("output.server") === server, "The server failed to be substitued")
    assert(httpConfig.getString("output.uri") === uri, "The uri failed to be substitued")
  }
}

trait BuildStoreAndPrefixConfig extends FunSpec with PrivateMethodTester {
  describe("buildStoreAndPrefixConfig") {
    describe("with empty string parameters") {
      it("should return a config with empty parameters") {
        val buildStoreAndPrefixConfig = PrivateMethod[Config]('buildStoreAndPrefixConfig)
        val config: Config = ProcessCacheService invokePrivate buildStoreAndPrefixConfig("", "", "", "")
        assert(config.getString("mogobiz.cache.uri.generic.apiPrefix") === "")
        assert(config.getString("mogobiz.cache.uri.generic.apiStore") === "")
        assert(config.getString("mogobiz.cache.uri.generic.frontPrefix") === "")
        assert(config.getString("mogobiz.cache.uri.generic.frontStore") === "")
      }
    }
    describe("with not empty string parameters") {
      it("should return a config with not empty parameters") {
        val buildStoreAndPrefixConfig = PrivateMethod[Config]('buildStoreAndPrefixConfig)
        val config: Config = ProcessCacheService invokePrivate buildStoreAndPrefixConfig("apiPrefix", "apiStore", "frontPrefix", "frontStore")
        assert(config.getString("mogobiz.cache.uri.generic.apiPrefix") === "apiPrefix")
        assert(config.getString("mogobiz.cache.uri.generic.apiStore") === "apiStore")
        assert(config.getString("mogobiz.cache.uri.generic.frontPrefix") === "frontPrefix")
        assert(config.getString("mogobiz.cache.uri.generic.frontStore") === "frontStore")
      }
    }
  }
}