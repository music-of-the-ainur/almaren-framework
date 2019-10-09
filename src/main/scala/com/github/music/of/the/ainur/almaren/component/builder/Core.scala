package com.github.music.of.the.ainur.almaren.component.builder

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Tree}
import com.github.music.of.the.ainur.almaren.component.builder.core.{Source, Main, Target}
import scala.language.implicitConversions
import com.github.music.of.the.ainur.almaren.component.Implicit._

trait Core {
  val tree: Option[Tree]

  implicit def state2ExistingTree(state: State): Option[Tree] =
    tree match {
      case Some(t) => Builder.addLeft(state,t)
      case None => Tree(state)
    }

  def fork(treeNodes: Option[Tree]*): List[Tree] = 
    tree match {
      case Some(t) => List(Builder.addRight(t,treeNodes.flatten.toList))
      case None => treeNodes.flatten.toList
    }
}

object Core {
  implicit class Implicit(val tree: Option[Tree]) extends Source with Main with Target
}
