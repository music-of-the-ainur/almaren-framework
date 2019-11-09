package com.github.music.of.the.ainur.almaren

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Almaren extends LazyLogging with Executor {
  def apply(appName: String) = { sparkConf.setAppName(appName);this}
  val sparkConf = new SparkConf
  lazy val spark = SparkSession.builder().config(sparkConf)
  val builder: Option[List[Container]] = None
}

trait Almaren
