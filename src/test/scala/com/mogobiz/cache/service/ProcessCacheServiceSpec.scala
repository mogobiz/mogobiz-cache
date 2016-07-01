package com.mogobiz.cache.service

//class ProcessCacheServiceSpec extends BuildStaticUrlsConfigSpec
//
//trait BuildStaticUrlsConfigSpec extends FunSpec with PrivateMethodTester {
//  describe("buildStaticUrlsConfig") {
//    describe("with an empty list of static url") {
//      it("should produce an empty Config") {
//        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig("apiStore", List())
//        assert(config === ConfigFactory.empty())
//      }
//    }
//
//    describe("with a list with empty static url") {
//      it("should produce an empty Config") {
//        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig("apiStore", List("", ""))
//        assert(config === ConfigFactory.empty())
//      }
//    }
//
//    describe("with a list of one static url") {
//      it("should append an element to the list mogobiz.cache.uri.generic.process") {
//        val url = "http://my.domain.com/customUrl"
//        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig("apiStore", List(url))
//        val configResolved: Config = config.withFallback(ConfigFactory.parseString(
//          """
//            mogobiz.cache.uri.generic.process:[]
//            mogobiz.cache.server.httpServer.maxClient: 10
//            mogobiz.cache.server.api: api
//          """)).resolve()
//        assert(configResolved.hasPath("mogobiz.cache.uri.generic.process"), "Doesn't contain the key: mogobiz.cache.uri.generic.process")
//        val configList = configResolved.getObjectList("mogobiz.cache.uri.generic.process")
//        assert(configList.size() === 1, "The list mogobiz.cache.uri.generic.process should have one element")
//        val httpConfig: Config = configList.get(0).toConfig
//        checkHttpConfig(httpConfig, "my.domain.com", 80, url)
//      }
//    }
//
//    describe("with a list of two static url") {
//      it("should append two element to the list mogobiz.cache.uri.generic.process in the same order as they have been declared") {
//        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
//        val url1 = "http://my.domain.com/url1"
//        val url2 = "http://my.domain.com/url2"
//        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig("apiStore", List(url1, url2))
//        val configResolved: Config = config.withFallback(ConfigFactory.parseString(
//          """
//            mogobiz.cache.uri.generic.process:[]
//            mogobiz.cache.server.api: api
//            mogobiz.cache.server.httpServer.maxClient: 10
//          """)).resolve()
//        assert(configResolved.hasPath("mogobiz.cache.uri.generic.process"), "Doesn't contain the key: mogobiz.cache.uri.generic.process")
//        val configList = configResolved.getObjectList("mogobiz.cache.uri.generic.process")
//        assert(configList.size() === 2, "The list mogobiz.cache.uri.generic.process should have one element")
//        checkHttpConfig(configList.get(0).toConfig, "my.domain.com", 80, url1)
//        checkHttpConfig(configList.get(1).toConfig, "my.domain.com", 80, url2)
//      }
//    }
//
//    describe("with a list of one dynamic url with one variable") {
//      it("should append an element to the list mogobiz.cache.uri.generic.process") {
//        val url = "http://my.domain.com/customUrl/${myIndex.myField}"
//        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig("apiStore", List(url))
//        val configResolved: Config = config.withFallback(ConfigFactory.parseString(
//          """
//            mogobiz.cache.uri.generic.process:[]
//            mogobiz.cache.server.es: es
//            mogobiz.cache.server.httpServer.maxClient: 10
//            myIndex.myField: "fieldValue"
//          """)).resolve()
//        assert(configResolved.hasPath("mogobiz.cache.uri.generic.process"), "Doesn't contain the key: mogobiz.cache.uri.generic.process")
//        val configList = configResolved.getObjectList("mogobiz.cache.uri.generic.process")
//        assert(configList.size() === 1, "The list mogobiz.cache.uri.generic.process should have one element")
//        val httpConfig: Config = configList.get(0).toConfig
//        checkHttpConfig(httpConfig, "my.domain.com", 80, url)
//      }
//    }
//
//    describe("with a list of one dynamic url with one ES type and two variable") {
//      it("should append an element to the list mogobiz.cache.uri.generic.process") {
//        val url = "http://my.domain.com/customUrl/${myIndex.myField1}/${myIndex.myField2}"
//        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig("apiStore", List(url))
//        val configResolved: Config = config.withFallback(ConfigFactory.parseString(
//          """
//            mogobiz.cache.uri.generic.process:[]
//            mogobiz.cache.server.es: es
//            mogobiz.cache.server.httpServer.maxClient: 10
//            myIndex.myField: "fieldValue"
//          """)).resolve()
//        assert(configResolved.hasPath("mogobiz.cache.uri.generic.process"), "Doesn't contain the key: mogobiz.cache.uri.generic.process")
//        val configList = configResolved.getObjectList("mogobiz.cache.uri.generic.process")
//        assert(configList.size() === 1, "The list mogobiz.cache.uri.generic.process should have one element")
//        val httpConfig: Config = configList.get(0).toConfig
//        checkHttpConfig(httpConfig, "my.domain.com", 80, url)
//      }
//    }
//
//    describe("with a list of one dynamic url with two ES type and one variable each") {
//      it("should return an empty config") {
//        val url = "http://my.domain.com/customUrl/${myFirstIndex.myField1}/${mySecondIndex.myField2}"
//        val buildStaticUrlsConfig = PrivateMethod[Config]('buildStaticUrlsConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStaticUrlsConfig("apiStore", List(url))
//        assert(config === ConfigFactory.empty())
//      }
//    }
//  }
//
//  def checkHttpConfig(httpConfig: Config, host: String, port:Int, uri: String): Unit = {
//    assert(httpConfig.getString("output.server.host") === host, "The host failed to be filled")
//    assert(httpConfig.getInt("output.server.port") === port, "The port failed to be filled")
//    assert(httpConfig.getString("output.uri") === uri, "The uri failed to be substitued")
//  }
//}

//trait BuildStoreAndPrefixConfig extends FunSpec with PrivateMethodTester {
//  describe("buildStoreAndPrefixConfig") {
//    describe("with empty string parameters") {
//      it("should return a config with empty parameters") {
//        val buildStoreAndPrefixConfig = PrivateMethod[Config]('buildStoreAndPrefixConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStoreAndPrefixConfig("", "", "", "")
//        assert(config.getString("mogobiz.cache.uri.generic.apiPrefix") === "")
//        assert(config.getString("mogobiz.cache.uri.generic.apiStore") === "")
//        assert(config.getString("mogobiz.cache.uri.generic.frontPrefix") === "")
//        assert(config.getString("mogobiz.cache.uri.generic.frontStore") === "")
//      }
//    }
//    describe("with not empty string parameters") {
//      it("should return a config with not empty parameters") {
//        val buildStoreAndPrefixConfig = PrivateMethod[Config]('buildStoreAndPrefixConfig)
//        val config: Config = ProcessCacheService invokePrivate buildStoreAndPrefixConfig("apiPrefix", "apiStore", "frontPrefix", "frontStore")
//        assert(config.getString("mogobiz.cache.uri.generic.apiPrefix") === "apiPrefix")
//        assert(config.getString("mogobiz.cache.uri.generic.apiStore") === "apiStore")
//        assert(config.getString("mogobiz.cache.uri.generic.frontPrefix") === "frontPrefix")
//        assert(config.getString("mogobiz.cache.uri.generic.frontStore") === "frontStore")
//      }
//    }
//  }
//}