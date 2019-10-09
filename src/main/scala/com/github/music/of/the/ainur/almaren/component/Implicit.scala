package com.github.music.of.the.ainur.almaren.component

package object Implicit {
  import scala.language.implicitConversions

  implicit def tree2optionn(tree: Tree): Option[Tree] = 
    Some(tree)
}
