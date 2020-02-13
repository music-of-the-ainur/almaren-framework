package com.github.music.of.the.ainur.almaren.builder

import com.github.music.of.the.ainur.almaren.builder.core.{Deserializer, Main, Source, Target}
import com.github.music.of.the.ainur.almaren.{Builder, Container, NullFork, State, Tree}
import zipper.Zipper

import scala.language.implicitConversions
import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.Executor

trait Core {
  val container: List[Container]

  private def newContainer(state:State): Zipper[Tree] =
    Zipper(Tree(state))
  
  implicit def state2ExistingTree(state: State): List[Container] = 
    if(!container.isEmpty)
      container.last.zipper match {
        case Some(z) => container.init :+ Container(Some(Builder.addLeft(state,z)))
        case None => container.init :+ Container(Some(newContainer(state)))
      }
      else
        newContainer(state)

  def fork(containers: List[Container]*): List[Container] = {
    container.init :+ Container(Some(
      Builder.addRight(container.last.zipper.getOrElse(throw NullFork())
        ,containers.flatten.map(_.last.zipper.getOrElse(throw NullFork()).commit).toList))) :+ Container(None)

  }
}

object Core {
  implicit class Implicit(val container: List[Container]) extends Source with Main with Target with Deserializer with Executor {
    def batch: DataFrame = 
      batch(container)

  }
}
