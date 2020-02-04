package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Container
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core._

private[almaren] trait Main extends Core {
  def sql(sql: String): List[Container] =
    Sql(sql)

  def alias(alias:String): List[Container] =
    Alias(alias)

  def cache(opType:Boolean = true,tableName:Option[String] = None): List[Container] = 
    Cache(opType, tableName)

  def coalesce(size:Int): List[Container] =
    Coalesce(size)

  def repartition(size:Int): List[Container] =
    Repartition(size)

  def pipe(command:String): List[Container] =
    Pipe(command)

  def dsl(dsl:String): List[Container] =
    Dsl(dsl)
}
