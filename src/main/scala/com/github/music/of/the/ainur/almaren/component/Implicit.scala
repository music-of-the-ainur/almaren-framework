package com.github.music.of.the.ainur.almaren.component

package object Implicit {
  import scala.language.implicitConversions
  
implicit def state2tree(state: State): Tree =
    Tree(state)
}
