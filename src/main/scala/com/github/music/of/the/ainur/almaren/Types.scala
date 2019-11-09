package com.github.music.of.the.ainur.almaren

import zipper.Zipper

private[almaren] case class Tree(state: State, c: List[Tree] = List.empty)
case class Container(zipper:Option[Zipper[Tree]])
