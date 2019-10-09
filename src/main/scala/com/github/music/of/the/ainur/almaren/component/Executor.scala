package com.github.music.of.the.ainur.almaren.component

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.github.music.of.the.ainur.almaren.Almaren

private[almaren] trait Executor {
  // execute's PreOrder BT
  def catalyst(container: Option[Container],df: DataFrame = Almaren.spark.getOrCreate().emptyDataFrame): DataFrame =
    catalyst(container.getOrElse(throw NullCatalyst()).zipper.commit,df)

  private def catalyst(tree: Tree,df: DataFrame): DataFrame = {
    tree match {
      case Tree(s, list) if list.nonEmpty => parentExec(list,s.executor(df))
      case Tree(s, list) => s.executor(df)
    }
  }
  private def parentExec(tree: List[Tree],df: DataFrame): DataFrame = 
    tree.foldLeft(df)((d,t) => catalyst(t,df))  
}
