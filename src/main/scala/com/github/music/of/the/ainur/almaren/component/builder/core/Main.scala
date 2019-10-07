package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Tree}
import com.github.music.of.the.ainur.almaren.component.state.core._
import com.github.music.of.the.ainur.almaren.component.builder.Core

private[ainur] trait Main extends Core {
  val tree: Tree

  def sql(sql: String): Tree =
    new Sql(sql)

  def alias(alias:String): Tree =
    new Alias(alias)

}
