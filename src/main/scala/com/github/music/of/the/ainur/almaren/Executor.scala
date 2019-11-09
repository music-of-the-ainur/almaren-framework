package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame

private[almaren] trait Executor extends Catalyst with Batch with Streaming

private trait Catalyst {
  // execute's PreOrder BT

  def catalyst(container: Option[List[Container]],df:DataFrame): DataFrame = 
    container.getOrElse(throw NullCatalyst()).foldLeft(df)((d,c) => c.zipper match {
      case Some(zipper) => catalyst(zipper.commit,d)
      case None => d
    })

  def catalyst(tree: Tree,df: DataFrame): DataFrame = {
    val nodeDf = tree.state.executor(df)
    tree.c match {
      case node :: Nil => catalyst(node,nodeDf)
      case Nil => nodeDf
      case nodes => nodesExecutor(nodes,nodeDf)
    }
  }

  private def nodesExecutor(tree: List[Tree],df: DataFrame): DataFrame = {
    tree.foldLeft(df)((d,t) => catalyst(t,df))
    df
  }
  
}

private trait Batch {
  this:Catalyst =>
  def batch(container: Option[List[Container]],df: DataFrame = Almaren.spark.getOrCreate().emptyDataFrame): DataFrame =
    catalyst(container,df)
}

private trait Streaming {
  this:Streaming =>
  def streaming(params:Map[String,String]): Unit = {
    Almaren.spark.getOrCreate().readStream.format("kafka").options(params)
  }
}
