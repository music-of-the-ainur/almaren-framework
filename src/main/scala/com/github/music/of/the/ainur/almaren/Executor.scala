package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame


private[almaren] trait Executor extends Catalyst with Batch with Streaming

private trait Catalyst {
  // execute's PreOrder BT
  def catalyst(tree: Tree,df: DataFrame): DataFrame = {
     tree match {
       case Tree(s, list) if list.nonEmpty => parentExec(list,s.executor(df))
       case Tree(s, list) => s.executor(df)
     }
   }
  private def parentExec(tree: List[Tree],df: DataFrame): DataFrame =
    tree.foldLeft(df)((d,t) => catalyst(t,df))
}

private trait Batch {
  this:Catalyst =>
  def batch(container: Option[Container],df: DataFrame = Almaren.spark.getOrCreate().emptyDataFrame): DataFrame =
    catalyst(container.getOrElse(throw NullCatalyst()).zipper.commit,df)
}

private trait Streaming {
  this:Streaming =>
  def streaming(params:Map[String,String]): Unit = {
    Almaren.spark.getOrCreate().readStream.format("kafka").options(params)
  }
}
