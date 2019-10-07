package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Tree}
import com.github.music.of.the.ainur.almaren.component.builder.Core
import com.github.music.of.the.ainur.almaren.component.state.core._

private[ainur] trait Target extends Core {
  val tree: Tree

  def targetSql(sql: String): Tree = new TargetSql(sql: String)
}
