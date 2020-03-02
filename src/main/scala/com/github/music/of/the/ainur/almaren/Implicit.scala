package com.github.music.of.the.ainur

package object almaren {
  import scala.language.implicitConversions

 implicit def zipper2optionTree(tree: Tree): Option[Tree] =
    Some(tree)

  private def newContainer(state:State): Tree =
    Tree(state)
 
}
