package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Tree}
import com.github.music.of.the.ainur.almaren.component.state.core._
private[ainur] object Main  {
  implicit class ImpMain(tree: Tree) {
    implicit def state2ExistingTree(state: State): Tree =
      Builder.addLeft(state,tree)

    def sql(sql: String): Tree =
      new Sql(sql)

    def alias(alias:String): Tree =
      new Alias(alias)

  }
}
