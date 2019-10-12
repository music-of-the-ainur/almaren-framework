package com.github.music.of.the.ainur.almaren.builder

import com.github.music.of.the.ainur.almaren.builder.core.{Main, Source, Target}
import com.github.music.of.the.ainur.almaren.{Builder, Container, NullFork, State, Tree}
import zipper.Zipper

import scala.language.implicitConversions

trait Core {
  val container: Option[Container]

  implicit def state2ExistingTree(state: State): Option[Container] =
    container match {
      case Some(c) => Builder.addLeft(state,c.zipper)
      case None => Container(Zipper(Tree(state)))
    }


  def fork(containers: Option[Container]*): Option[Container] = 
    container match {
      case Some(c) => Builder.addRight(c.zipper,containers.flatten.map(_.zipper.commit).toList)
      case None => throw NullFork()
    }
}

object Core {
  implicit class Implicit(val container: Option[Container]) extends Source with Main with Target
}
