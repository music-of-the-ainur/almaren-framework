package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.state.core._
import com.github.music.of.the.ainur.almaren.component.{Builder, State, Tree}

private[ainur] object Target  {
  implicit class ImpMain(tree: Tree) {
    implicit def state2ExistingTree(state: State): Tree =
      Builder.addLeft(state,tree)

    def targetSql(sql: String): Tree = new TargetSql(sql: String)

  }
}
