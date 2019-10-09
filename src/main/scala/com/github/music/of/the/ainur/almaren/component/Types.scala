package com.github.music.of.the.ainur.almaren.component

import zipper.Zipper

private[almaren] case class Tree(state: State, c: List[Tree] = List.empty)
private[almaren] case class Container(zipper:Zipper[Tree])
