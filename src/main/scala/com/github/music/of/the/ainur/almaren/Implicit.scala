package com.github.music.of.the.ainur

import zipper.Zipper

package object almaren {
  import scala.language.implicitConversions

  implicit def listContainer2optionList(container: List[Container]): Option[List[Container]] =
    Some(container)

  implicit def container2option(container: Container): Option[List[Container]] = 
    Some(List(container))

  implicit def zipper2containerOptionList(zipper: Zipper[Tree]): Option[List[Container]] =
    Some(List(Container(Some(zipper))))
}
