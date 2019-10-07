package com.github.music.of.the.ainur.almaren.component.state.core

import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.component.State

private[ainur] abstract class Source() extends State {
  override def executor(df: DataFrame): DataFrame = source(df)
  def source(df: DataFrame): DataFrame
}

class SourceSql(sql: String) extends Source {
  override def source(df: DataFrame): DataFrame = {
    logger.info(s"sql:{$sql}")
    val sqlDf = df.sparkSession.sql(sql)
    sqlDf
  }
}

class SourceJdbc(url: String, driver: String, query: String, params:Map[String,String] = Map[String,String]()) extends Source {
  override def source(df: DataFrame): DataFrame = {
    logger.info(s"sql:{$url}, driver:{$driver}, params:{$params}")
    df.sparkSession.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("query", query)
      .options(params)
    df
  }
}
