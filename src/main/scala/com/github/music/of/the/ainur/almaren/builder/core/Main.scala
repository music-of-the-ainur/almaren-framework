package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core._
import org.apache.spark.sql.Column
import org.apache.spark.storage.StorageLevel

private[almaren] trait Main extends Core {
  def sql(sql: String): Option[Tree] =
    Sql(sql)

  def alias(alias:String): Option[Tree] =
    Alias(alias)

  def cache(opType:Boolean = true,tableName:Option[String] = None,storageLevel: Option[StorageLevel] = None): Option[Tree] =
    Cache(opType, tableName = tableName, storageLevel = storageLevel)

  def coalesce(size:Int): Option[Tree] =
    Coalesce(size)

  def repartition(size:Int): Option[Tree] =
    Repartition(size)

  def repartition(partitionExprs: Column*): Option[Tree] =
    RepartitionWithColumn(partitionExprs:_*)

  def repartition(size: Int, partitionExprs: Column*): Option[Tree] =
    RepartitionWithSizeAndColumn(size, partitionExprs:_*)

  def pipe(command:String*): Option[Tree] =
    Pipe(command:_*)

  def dsl(dsl:String): Option[Tree] =
    Dsl(dsl)

  def sqlExpr(exprs:String*): Option[Tree] =
    SqlExpr(exprs:_*)

  def where(expr:String): Option[Tree] =
    Where(expr)

  def drop(drop:String*): Option[Tree] =
    Drop(drop:_*)
}