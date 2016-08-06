name := "mogobiz-cache"

organization in ThisBuild := "com.mogobiz"

//version in ThisBuild := "0.1-SNAPSHOT"

logLevel in Global := Level.Info

scalaVersion := "2.11.6"

crossScalaVersions in ThisBuild := Seq("2.11.6")

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

resolvers in ThisBuild ++= Seq(
    Resolver.sonatypeRepo("releases"),
"ebiz repo" at "http://art.ebiznext.com/artifactory/libs-release-local",
"Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
)

git.useGitDescribe := true

git.gitUncommittedChanges := false

val akkaStreamV = "1.0"

val sprayV = "1.3.3"

val scalaLoggingV = "3.4.0"

val slf4jLog4jV = "1.7.12"

val scalaTestV = "3.0.0-M16-SNAP6"

libraryDependencies in ThisBuild ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "com.typesafe.akka" %% "akka-stream-experimental" % akkaStreamV,
  "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingV,
  "org.slf4j" % "slf4j-log4j12" % slf4jLog4jV,
  "org.scalatest" % "scalatest_2.11" % scalaTestV,
  "com.squareup.okhttp3" % "okhttp" % "3.4.1"
)

enablePlugins(GitVersioning, GitBranchPrompt)

mainClass in assembly := Some("com.mogobiz.cache.bin.ProcessCache")

assemblyJarName in assembly := name.value + "-" + version.value + ".jar"

assemblyMergeStrategy in assembly := {
  case "application.conf" => MergeStrategy.discard
  case "log4j.xml" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

isSnapshot in ThisBuild := version.value.trim.endsWith("SNAPSHOT")

publishTo in ThisBuild := {
  val artifactory = "http://art.ebiznext.com/artifactory/"
  if (isSnapshot.value)
    Some("snapshots" at artifactory + "libs-snapshot-local")
  else
    Some("releases" at artifactory + "libs-release-local")
}

credentials in ThisBuild += Credentials(Path.userHome / ".ivy2" / ".credentials")

publishArtifact in(ThisBuild, Compile, packageSrc) := false

publishArtifact in(ThisBuild, Test, packageSrc) := false

publishMavenStyle in ThisBuild := true

publishArtifact in (ThisBuild, Test) := false

pomIncludeRepository := { _ => false }

publish <<= publish dependsOn assembly

publishM2 <<= publishM2 dependsOn assembly

publishLocal <<= publishLocal dependsOn assembly

val packageStandalone = taskKey[File]("package-standalone")

packageStandalone := (baseDirectory in Compile).value / "target" / "scala-2.11" / (name.value + "-" + version.value + ".jar")

addArtifact( Artifact("mogobiz-cache", "standalone"), packageStandalone )
