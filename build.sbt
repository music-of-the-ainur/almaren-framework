ThisBuild / name := "almaren-framework"
ThisBuild / organization := "com.github.music-of-the-ainur"

lazy val scala212 = "2.12.10"
lazy val scala213 = "2.13.9"

crossScalaVersions := Seq(scala212,scala213)
ThisBuild / scalaVersion := scala213

val sparkVersion = "3.3.0"
val majorVersionReg = "([0-9]+\\.[0-9]+).{0,}".r

val majorVersionReg(majorVersion) = sparkVersion

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.14.0",
  "com.github.music-of-the-ainur" %% "quenya-dsl" % s"1.2.1-${majorVersion}",
  "org.scalatest" %% "scalatest" % "3.2.14" % "test",
  "org.postgresql" % "postgresql" % "42.2.8" % "test"
)

enablePlugins(GitVersioning)

resolvers +=
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/service/local/repositories/releases/content"

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/music-of-the-ainur/almaren-framework"),
    "scm:git@github.com:music-of-the-ainur/almaren-framework.git"
  )
)
ThisBuild / developers := List(
  Developer(
    id    = "mantovani",
    name  = "Daniel Mantovani",
    email = "daniel.mantovani@modak.com",
    url   = url("https://github.com/music-of-the-ainur")
  ),
  Developer(
    id = "badrinathpatchikolla",
    name = "Badrinath Patchikolla",
    email = "badrinath.patchikolla@modakanalytics.com",
    url = url("https://github.com/music-of-the-ainur")
  )
)

ThisBuild / description := "The Almaren Framework provides a simplified consistent minimalistic layer over Apache Spark. While still allowing you to take advantage of native Apache Spark features. You can still combine it with standard Spark code"
ThisBuild / licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/music-of-the-ainur/almaren-framework"))
ThisBuild / organizationName := "Music of Ainur"
ThisBuild / organizationHomepage := Some(url("https://github.com/music-of-the-ainur"))


// Remove all additional repository other than Maven Central from POM
credentials += Credentials(Path.userHome / ".sbt" / "sonatype_credential")
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

ThisBuild / publishMavenStyle := true
updateOptions := updateOptions.value.withGigahorse(false)
