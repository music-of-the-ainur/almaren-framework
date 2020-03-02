package com.github.music.of.the.ainur.almaren.builder

import com.github.music.of.the.ainur.almaren.builder.core.{Deserializer, Main, Source, Target}
import com.github.music.of.the.ainur.almaren.{NullFork, State, Tree}

import scala.language.implicitConversions
import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.Executor
import com.github.music.of.the.ainur.almaren.NullCatalyst

trait Core {
  val container: Option[Tree]

  import scala.language.implicitConversions
 
  implicit def state2Tree(state: State): Option[Tree] = 
    container match {
      case Some(t) => t.copy(c= t.c :+ Tree(state))
      case None => Tree(state) 
    }

  def fork(containers: Option[Tree]*): Option[Tree] = {
    val cr = container.getOrElse(throw NullCatalyst)
    val tree = cr.c.last.copy(c = containers.flatMap(c => c).toList)
    cr.copy(c = cr.c.init :+ tree)
  }
}

object Core {
  implicit class Implicit(val container: Option[Tree]) extends Source with Main with Target with Deserializer with Executor {
    def batch: DataFrame = 
      batch(container)

  }
}
