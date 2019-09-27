package com.github.music.of.the.ainur.almaren

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

trait State extends LazyLogging with Serializable {
  def state(df: DataFrame): DataFrame
}
