package com.github.music.of.the.ainur.almaren.state.core

import com.github.music.of.the.ainur.almaren.State
import com.github.music.of.the.ainur.almaren.util.Constants
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

private[almaren] abstract class Main extends State {
  override def executor(df: DataFrame): DataFrame = core(df)
  def core(df: DataFrame): DataFrame
}


case class Dsl(dslLang:String) extends Main {

  import com.github.music.of.the.ainur.quenya.QuenyaDSL
  val quenyaDsl = QuenyaDSL
  val dsl = quenyaDsl.compile(dslLang)

  override def core(df: DataFrame): DataFrame = dsl(df)
  def dsl(df: DataFrame): DataFrame = {
    logger.info(s"dsl:{$dslLang}")
    quenyaDsl.execute(dsl,df)
  }
}

case class Sql(sql: String) extends Main {
  override def core(df: DataFrame): DataFrame = sql(df)
  def sql(df: DataFrame): DataFrame = {
    logger.info(s"sql:{$sql}")
    df.sqlContext.sql(sql)
  }
}

case class Coalesce(size:Int) extends Main {
  override def core(df: DataFrame): DataFrame = coalesce(df)
  def coalesce(df: DataFrame): DataFrame = {
    logger.info(s"{$size}")
    df.coalesce(size)
  }
}

case class Repartition(size:Int) extends Main {
  override def core(df: DataFrame): DataFrame = repartition(df)
  def repartition(df: DataFrame): DataFrame = {
    logger.info(s"{$size}")
    df.repartition(size)
  }
}

case class RepartitionWithColumn(partitionExprs:Column*) extends Main {
  override def core(df: DataFrame): DataFrame = repartition(df)
  def repartition(df: DataFrame): DataFrame = {
    logger.info(s"{${partitionExprs.mkString("\n")}}")
    df.repartition(partitionExprs:_*)
  }
}

case class RepartitionWithSizeAndColumn(size: Int,partitionExprs:Column*) extends Main {
  override def core(df: DataFrame): DataFrame = repartition(df)
  def repartition(df: DataFrame): DataFrame = {
    logger.info(s"{$size},{${partitionExprs.mkString("\n")}}")
    df.repartition(size,partitionExprs:_*)
  }
}

case class Pipe(command:String*) extends Main {
  override def core(df: DataFrame): DataFrame = pipe(df)
  def pipe(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"{$command}")
    df.rdd.pipe(command).toDF()
  }
}

case class Alias(alias:String) extends Main {
  override def core(df: DataFrame): DataFrame = alias(df)
  def alias(df: DataFrame): DataFrame = {
    logger.info(s"{$alias}")
    df.createOrReplaceTempView(alias)
    df
  }
}

case class Cache(opType: Boolean = true, tableName: Option[String] = None, storageLevel: Option[StorageLevel] = None) extends Main {
  override def core(df: DataFrame): DataFrame = cache(df)

  def cache(df: DataFrame): DataFrame = {
    logger.info(s"opType:{$opType}, tableName:{$tableName}, StorageType:{$storageLevel}")
    tableName match {
      case Some(t) => cacheTable(df, t)
      case None => cacheDf(df, storageLevel)
    }
    df
  }

  private def cacheDf(df: DataFrame, storageLevel: Option[StorageLevel]): Unit = opType match {
    case true => {
      storageLevel match {
        case Some(value) => df.persist(value)
        case None => df.persist()
      }
    }
    case false => df.unpersist()

  }

  private def cacheTable(df: DataFrame, tableName: String): Unit =
    opType match {
      case true => df.sqlContext.cacheTable(tableName)
      case false => df.sqlContext.uncacheTable(tableName)
    }
}

case class SqlExpr(exprs:String*) extends Main {
  override def core(df: DataFrame): DataFrame = {
    logger.info(s"""exprs:{${exprs.mkString("\n")}}""")
    df.selectExpr(exprs:_*)
  }
}

case class Where(where:String) extends Main {
  override def core(df: DataFrame): DataFrame = {
    logger.info(s"where:{$where}")
    df.where(where)
  }
}

case class Drop(drop:String*) extends Main {
  override def core(df: DataFrame): DataFrame = {
    logger.info(s"""drop:{${drop.mkString("\n")}}""")
    df.drop(drop:_*)
  }
}