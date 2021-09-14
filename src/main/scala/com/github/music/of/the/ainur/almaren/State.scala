package com.github.music.of.the.ainur.almaren

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.util.Utils

private[almaren] trait State extends LazyLogging with Serializable {
  def executor(df: DataFrame): DataFrame
  def _executor(df: DataFrame): DataFrame = {
    val state = executor(df)
    logger.whenDebugEnabled(state.show)
    state
  }

}
