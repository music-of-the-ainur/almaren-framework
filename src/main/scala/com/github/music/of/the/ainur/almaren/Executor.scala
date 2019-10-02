package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame

trait Executor {
  def catalyst(tree: Tree,df: DataFrame = Almaren.spark.getOrCreate().emptyDataFrame): DataFrame = {
    tree match {
      case Tree(s, list) if list.nonEmpty => list.foldLeft(tree.state.executor(df))((d, t) => catalyst(t,d))
      case Tree(s, list) => tree.state.executor(df)
    }
  }
}
