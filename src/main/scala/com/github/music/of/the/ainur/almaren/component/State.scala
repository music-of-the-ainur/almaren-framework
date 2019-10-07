package com.github.music.of.the.ainur.almaren.component

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

private[almaren] trait State extends LazyLogging with Serializable {
  def executor(df: DataFrame): DataFrame
}
