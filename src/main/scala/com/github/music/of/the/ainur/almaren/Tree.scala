package com.github.music.of.the.ainur.almaren

sealed abstract class Tree
case class NodeItem(node: State) extends Tree
case class NodeList(node: List[Tree]) extends Tree
case class Node(node: Tree) extends Tree
