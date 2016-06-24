name := "mogobiz-cache"

organization in ThisBuild := "com.mogobiz"

version in ThisBuild := "0.1-SNAPSHOT"

logLevel in Global := Level.Info

scalaVersion := "2.11.6"

crossScalaVersions in ThisBuild := Seq("2.11.6")

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers in ThisBuild ++= Seq(
    Resolver.sonatypeRepo("releases"),
"ebiz repo" at "http://art.ebiznext.com/artifactory/libs-release-local",
"Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
)

val akkaStreamV = "1.0"

val sprayV = "1.3.3"

val scalaLoggingV = "3.4.0"

val slf4jLog4jV = "1.7.12"

val scalaTestV = "3.0.0-M16-SNAP6"

libraryDependencies in ThisBuild ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
  "io.spray" %% "spray-client" % sprayV,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
  "org.slf4j" % "slf4j-log4j12" % slf4jLog4jV,
  "org.scalatest" % "scalatest_2.11" % scalaTestV
)

mainClass in assembly := Some("com.mogobiz.cache.bin.ProcessCache")

assemblyJarName in assembly := name.value + "-" + version.value + ".jar"

assemblyMergeStrategy in assembly := {
  case "application.conf" => MergeStrategy.discard
  case "log4j.xml" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

enablePlugins(UniversalPlugin)

import NativePackagerHelper._

mappings in Universal <++= sourceDirectory map( src => directory(src / "samples"))

mappings in Universal := {
  // universalMappings: Seq[(File,String)]
  val universalMappings = (mappings in Universal).value
  val fatJar = (assembly in Compile).value
  // removing means filtering
  val filtered = universalMappings filter {
    case (file, name) =>  ! name.endsWith(".jar")
  }
  // add the fat jar
  filtered :+ (fatJar -> ("lib/" + fatJar.getName))
}