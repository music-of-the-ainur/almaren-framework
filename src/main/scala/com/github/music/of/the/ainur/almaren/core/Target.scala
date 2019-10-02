package com.github.music.of.the.ainur.almaren.core

import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.util.Constants
import com.github.music.of.the.ainur.almaren.State

private[almaren] abstract class Target extends State {
  override def executor(df: DataFrame): DataFrame = target(df)
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

class TargetJdbc(url: String, driver: String, query: String, params:Map[String,String]) extends Target {
  override def target(df: DataFrame): DataFrame = {
    logger.info(s"sql:{$url}, driver:{$driver}, params:{$params}")
    df.write.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("query", query)
      .options(params)
    df
  }
}
