name := "mogobiz-cache"

organization in ThisBuild := "com.mogobiz"

version in ThisBuild := "0.1-SNAPSHOT"

logLevel in Global := Level.Info

crossScalaVersions in ThisBuild := Seq("2.11.6")

resolvers in ThisBuild ++= Seq(
    Resolver.sonatypeRepo("releases"),
"ebiz repo" at "http://art.ebiznext.com/artifactory/libs-release-local",
"Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
)

val akkaStreamV = "1.0"

val elastic4sV = "1.7.4"

val sprayV = "1.3.3"
libraryDependencies in ThisBuild ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
  "com.typesafe.akka" %% "akka-http-core-experimental" % akkaStreamV,
  "com.typesafe.akka" %% "akka-http-experimental" % akkaStreamV,
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % elastic4sV,
  "io.spray" %% "spray-client" % sprayV
)

