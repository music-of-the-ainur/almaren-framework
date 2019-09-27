package com.github.music.of.the.ainur.almaren.core

import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.State
import com.github.music.of.the.ainur.almaren.util.Constants

private[almaren] abstract class Core extends State {
  override def state(df: DataFrame): DataFrame = core(df)
  def core(df: DataFrame): DataFrame
}

class SQLState(sql: String) extends Core {
  override def core(df: DataFrame): DataFrame = sql(df)
  def sql(df: DataFrame): DataFrame = {
    logger.info(s"sql:{$sql}")
    df.createOrReplaceTempView(Constants.TempTableName)
    df.sqlContext.sql(sql)
  }
}

class Coalesce(size:Int) extends Core {
  override def core(df: DataFrame): DataFrame = coalesce(df)
  def coalesce(df: DataFrame): DataFrame = {
    logger.info(s"{$size}")
    df.coalesce(size)
  }
}

class Repartition(size:Int) extends Core {
  override def core(df: DataFrame): DataFrame = repartition(df)
  def repartition(df: DataFrame): DataFrame = {
    logger.info(s"{$size}")
    df.repartition(size)
  }
}

class Pipe(command:String) extends Core {
  override def core(df: DataFrame): DataFrame = pipe(df)
  def pipe(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"{$command}")
    df.rdd.pipe(command).toDF()
  }
}

class Alias(alias:String) extends Core {
  override def core(df: DataFrame): DataFrame = alias(df)
  def alias(df: DataFrame): DataFrame = {
    logger.info(s"{$alias}")
    df.createOrReplaceTempView(alias)
    df
  }
}

class Cache(opType:Boolean,tableName:Option[String]) extends Core {
  override def core(df: DataFrame): DataFrame = cache(df)
  def cache(df: DataFrame): DataFrame = {
    logger.info(s"opType:{$opType}, tableName{$tableName}")
    tableName match {
      case Some(t) => cacheTable(df,t)
      case None => cacheDf(df)
    }
    df
  }
  private def cacheDf(df:DataFrame): Unit = opType match {
    case true => df.cache()
    case false => df.unpersist()
  }
  private def cacheTable(df:DataFrame,tableName: String): Unit =
    opType match {
      case true => df.sqlContext.cacheTable(tableName)
      case false => df.sqlContext.uncacheTable(tableName)
    }
}
