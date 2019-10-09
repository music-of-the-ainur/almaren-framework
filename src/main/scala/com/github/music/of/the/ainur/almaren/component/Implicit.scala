package com.github.music.of.the.ainur.almaren.component

import zipper.Zipper

package object Implicit {
  import scala.language.implicitConversions

  implicit def container2optionn(container: Container): Option[Container] = 
    Some(container)

  implicit def zipper2container(zipper: Zipper[Tree]): Option[Container] =
    Some(Container(zipper))
}
