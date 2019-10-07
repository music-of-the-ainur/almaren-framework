package com.github.music.of.the.ainur.almaren

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.github.music.of.the.ainur.almaren.component.builder.core.Source
import com.github.music.of.the.ainur.almaren.component.Executor

object Almaren extends LazyLogging with Executor with Source {
  def apply(appName: String) = { sparkConf.setAppName(appName);Almaren}
  val sparkConf = new SparkConf
  lazy val spark = SparkSession.builder().config(sparkConf)
}

trait Almaren
