package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Container
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core._

private[almaren] trait Main extends Core {
  def sql(sql: String): Option[Container] =
    new Sql(sql)

  def alias(alias:String): Option[Container] =
    new Alias(alias)

  def cache(opType:Boolean = true,tableName:Option[String] = None): Option[Container] = 
    new Cache(opType, tableName)

  def coalesce(size:Int): Option[Container] =
    new Coalesce(size:Int)

  def repartition(size:Int): Option[Container] =
    new Repartition(size:Int)

  def pipe(command:String): Option[Container] =
    new Pipe(command:String)

  def dsl(dsl:String): Option[Container] =
    new Dsl(dsl:String)
}