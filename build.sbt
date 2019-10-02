name := "almaren-framework"
organization := "com.github.music.of.the.ainur.almaren"

scalaVersion := "2.12.8"
val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "io.github.stanch" %% "zipper" % "0.5.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided" excludeAll(ExclusionRule(organization = "net.jpountz.lz4")),
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.databricks" %% "spark-xml" % "0.6.0",

  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

enablePlugins(GitVersioning)
