package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame

private[almaren] sealed abstract class Tree
case class Node(state: State, left: Tree = Leaf, right: Tree = Leaf) extends Tree
case object Leaf extends Tree
