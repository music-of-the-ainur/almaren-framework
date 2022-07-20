package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.{TargetFile, TargetJdbc, TargetSql, TargetKafka}
import org.apache.spark.sql.SaveMode

private[almaren] trait Target extends Core {
  def targetSql(sql: String): Option[Tree] =
    TargetSql(sql)

  def targetJdbc(url: String, driver: String, dtable: String, saveMode: SaveMode = SaveMode.ErrorIfExists, user: Option[String] = None, password: Option[String] = None, params: Map[String, String] = Map()): Option[Tree] =
    TargetJdbc(url, driver, dtable, user, password, saveMode, params)

  def targetFile(format: String,
                 path: String,
                 saveMode: SaveMode = SaveMode.Overwrite,
                 params: Map[String, String] = Map(),
                 partitionBy: List[String] = List.empty,
                 bucketBy: (Int, List[String]) = (64, List.empty),
                 sortBy: List[String] = List.empty,
                 tableName: Option[String]): Option[Tree] =
    TargetFile(format, path, params, saveMode, partitionBy, bucketBy, sortBy, tableName)
}