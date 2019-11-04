ThisBuild / name := "almaren-framework"
ThisBuild / organization := "com.github.music.of.the.ainur.almaren"

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"

crossScalaVersions := Seq(scala211, scala212)
ThisBuild / scalaVersion := scala212

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "io.github.stanch" %% "zipper" % "0.5.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided" excludeAll(ExclusionRule(organization = "net.jpountz.lz4")),
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.6.0",
  "com.modakanalytics.quenya" %% "quenya-dsl" % "1.1.0-2.4-alpha.10",

  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.postgresql" % "postgresql" % "42.2.8" % "test"
)

enablePlugins(GitVersioning)
