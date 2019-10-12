package com.github.music.of.the.ainur

import zipper.Zipper

package object almaren {
  import scala.language.implicitConversions

  implicit def container2optionn(container: Container): Option[Container] = 
    Some(container)

  implicit def zipper2container(zipper: Zipper[Tree]): Option[Container] =
    Some(Container(zipper))
}
