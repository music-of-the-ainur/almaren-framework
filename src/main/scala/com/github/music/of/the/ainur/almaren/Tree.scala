package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame

private[almaren] case class Tree(state: State, c: List[Tree] = List.empty)
