name := "almaren-framework"
organization := "com.github.music.of.the.ainur.almaren"

scalaVersion := "2.11.8"
val sparkVersion = "2.3.0"

//ensimeScalaVersion in ThisBuild := scalaVersion.value

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion % "provided" excludeAll(ExclusionRule(organization = "net.jpountz.lz4")),

  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

enablePlugins(GitVersioning)
