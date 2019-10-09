package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Tree}
import com.github.music.of.the.ainur.almaren.component.state.core._
import com.github.music.of.the.ainur.almaren.component.builder.Core

private[almaren] trait Main extends Core {
  def sql(sql: String): Option[Tree] =
    new Sql(sql)

  def alias(alias:String): Option[Tree] =
    new Alias(alias)

  def cache(opType:Boolean = true,tableName:Option[String] = None): Option[Tree] = 
    new Cache(opType, tableName)
}
