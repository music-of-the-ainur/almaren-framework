package com.github.music.of.the.ainur.almaren.core

import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.util.Constants

abstract class Target extends State {
  override def state(df: DataFrame): DataFrame = target(df)
  def target(df: DataFrame): DataFrame
}

class TargetSql(sql: String) extends Target {
  override def target(df: DataFrame): DataFrame = {
    logger.info(s"sql:{$sql}")
    df.createOrReplaceTempView(Constants.TempTableName)
    val sqlDf = df.sqlContext.sql(sql)
    df
  }
}
