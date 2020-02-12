package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame

private[almaren] trait Executor extends Catalyst with Batch with Streaming

private trait Catalyst {
  // execute's PreOrder BT

  def catalyst(container: List[Container],df:DataFrame = Almaren.spark.getOrCreate().emptyDataFrame): DataFrame = 
    container.foldLeft(df)((d,c) => c.zipper match {
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
  def batch(container: List[Container]): DataFrame =
    catalyst(container)
}

private trait Streaming {
this:Catalyst => 
  def streaming(container: List[Container],params:Map[String,String] = Map()): Unit = {
    val spark = Almaren.spark.getOrCreate()

    import spark.implicits._

    val streamingDF = spark
      .readStream
      .format("kafka")
      .options(params)
      .load()

    val streaming = streamingDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      val df = catalyst(container,batchDF)
    }.start()

    streaming.awaitTermination()
  }
}
