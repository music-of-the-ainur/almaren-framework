package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Container}
import com.github.music.of.the.ainur.almaren.component.state.core._
import com.github.music.of.the.ainur.almaren.component.builder.Core

private[almaren] trait Main extends Core {
  def sql(sql: String): Option[Container] =
    new Sql(sql)

  def alias(alias:String): Option[Container] =
    new Alias(alias)

  def cache(opType:Boolean = true,tableName:Option[String] = None): Option[Container] = 
    new Cache(opType, tableName)
}
