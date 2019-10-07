package com.github.music.of.the.ainur.almaren.component.builder

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Tree}
import com.github.music.of.the.ainur.almaren.component.builder.core.{Source, Main, Target}

trait Core {
  val tree: Tree

  implicit def state2ExistingTree(state: State): Tree =
    Builder.addLeft(state,tree)

  def fork(treeNodes: Tree*): Tree = 
    treeNodes.foldLeft(tree)((t,td) => Builder.addRight(t,td))
}

object Core {
  implicit class Implicit(val tree: Tree) extends Source with Main with Target
}
