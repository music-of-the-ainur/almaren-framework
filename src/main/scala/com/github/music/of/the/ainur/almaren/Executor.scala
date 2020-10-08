package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.util.Constants
import com.typesafe.scalalogging.LazyLogging

private[almaren] trait Executor extends Catalyst with Batch with Streaming

private trait Catalyst extends LazyLogging {
  // execute's PreOrder BT

  def catalyst(tree: Option[Tree],df:DataFrame = Almaren.spark.getOrCreate().emptyDataFrame): DataFrame = {
    val t = tree.getOrElse(throw NullCatalyst)
    t.c.foldLeft(t.state._executor(df))((d,container) => catalyst(container,d))
  }

  def catalyst(tree: Tree,df: DataFrame): DataFrame = {
    val nodeDf = tree.state._executor(df)
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
  def batch(tree: Option[Tree]): DataFrame =
    catalyst(tree)
}

private trait Streaming {
this:Catalyst => 
  def streaming(tree: Option[Tree],params:Map[String,String] = Map()): Unit = {
    val spark = Almaren.spark.getOrCreate()

    import spark.implicits._

    val streamingDF = spark
      .readStream
      .format("kafka")
      .options(params)
      .load()

    val streaming = streamingDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF.createOrReplaceTempView(Constants.TempStreamTableName)
      val df = catalyst(tree,batchDF)
    }.start()

    streaming.awaitTermination()
  }
}
