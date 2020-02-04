package com.github.music.of.the.ainur

import zipper.Zipper

package object almaren {
  import scala.language.implicitConversions

  implicit def container2option(container: Container): List[Container] = 
    List(container)

  implicit def zipper2containerOptionList(zipper: Zipper[Tree]): List[Container] =
    List(Container(Some(zipper)))
}
