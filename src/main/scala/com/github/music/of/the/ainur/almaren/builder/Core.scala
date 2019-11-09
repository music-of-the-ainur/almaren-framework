package com.github.music.of.the.ainur.almaren.builder

import com.github.music.of.the.ainur.almaren.builder.core.{Main, Source, Target, Deserializer}
import com.github.music.of.the.ainur.almaren.{Builder, Container, NullFork, State, Tree}
import zipper.Zipper

import scala.language.implicitConversions

trait Core {
  val container: Option[List[Container]]

  private def newContainer(state:State): Zipper[Tree] = 
    Zipper(Tree(state))

  implicit def state2ExistingTree(state: State): Option[List[Container]] =
    container match {
      case Some(c) => 
        c.init :+ Container(Some(Builder.addLeft(state,c.last.zipper.getOrElse(newContainer(state)))))
      case None => newContainer(state)
    }

  def fork(containers: Option[List[Container]]*): Option[List[Container]] = 
    container match {
      case Some(c) => 
        c.init :+
        Container(Some(Builder.addRight(c.last.zipper.getOrElse(throw NullFork())
          ,containers.flatten.map(_.last.zipper.getOrElse(throw NullFork()).commit).toList))) :+ Container(None)
      case None => throw NullFork()
    }
}

object Core {
  implicit class Implicit(val container: Option[List[Container]]) extends Source with Main with Target with Deserializer
}
