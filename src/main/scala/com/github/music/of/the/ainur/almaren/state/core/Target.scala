package com.github.music.of.the.ainur.almaren.state.core

import com.github.music.of.the.ainur.almaren.State
import com.github.music.of.the.ainur.almaren.util.Constants
import org.apache.spark.sql.{DataFrame, SaveMode}

private[almaren] abstract class Target extends State {
  override def executor(df: DataFrame): DataFrame = target(df)

  def target(df: DataFrame): DataFrame
}

case class TargetSql(sql: String) extends Target {
  override def target(df: DataFrame): DataFrame = {
    logger.info(s"sql:{$sql}")
    val sqlDf = df.sqlContext.sql(sql)
    df
  }
}

case class TargetJdbc(url: String, driver: String, dbtable: String, user: Option[String], password: Option[String], saveMode: SaveMode, params: Map[String, String]) extends Target {
  override def target(df: DataFrame): DataFrame = {
    logger.info(s"url:{$url}, driver:{$driver}, dbtable:{$dbtable}, user:{$user}, params:{$params}")

    val options = (user, password) match {
      case (Some(user), None) => params + ("user" -> user)
      case (Some(user), Some(password)) => params + ("user" -> user, "password" -> password)
      case (_, _) => params
    }

    df.write.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("dbtable", dbtable)
      .options(options)
      .mode(saveMode)
      .save()
    df
  }
}

case class TargetKafka(servers: String, options: Map[String, String]) extends Target {
  override def target(df: DataFrame): DataFrame = {
    logger.info(s"options: $options")
    df.write
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .options(options)
      .save()
    df
  }
}

case class TargetFile(format: String,
                      path: String,
                      params: Map[String, String],
                      saveMode: SaveMode,
                      partitionBy: List[String],
                      bucketBy: (Int, List[String]),
                      sortBy: List[String],
                      tableName: Option[String]) extends Target {
  override def target(df: DataFrame): DataFrame = {
    logger.info(s"format:{$format}, path:{$path}, params:{$params}, partitionBy:{$partitionBy}, bucketBy:{$bucketBy}, sort:{$sortBy},tableName:{$tableName}")
    val write = df.write
      .format(format)
      .option("path", path)
      .options(params)
      .mode(saveMode)

    if (partitionBy.nonEmpty)
      write.partitionBy(partitionBy: _*)

    if (bucketBy._2.nonEmpty) {
      write.bucketBy(bucketBy._1, bucketBy._2.head, bucketBy._2.tail: _*)
      if (sortBy.nonEmpty)
        write.sortBy(sortBy.head, sortBy.tail: _*)
    }

    tableName match {
      case Some(tableName) => write.saveAsTable(tableName)
      case _ => write.save
    }

    df
  }
}
