package com.github.music.of.the.ainur.almaren.component

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.github.music.of.the.ainur.almaren.Almaren

private[ainur] trait Executor {
  // execute's PreOrder BT
  def catalyst(tree: Tree,df: DataFrame = Almaren.spark.getOrCreate().emptyDataFrame, count:Int = 0): DataFrame = {
    println(s"[+]$count")
    tree match {
      case Tree(s, list) if list.nonEmpty => parentExec(list,s.executor(df),count + 1)
      case Tree(s, list) => s.executor(df)
    }
  }
  private def parentExec(tree: List[Tree],df: DataFrame, count:Int): DataFrame = 
    tree.foldLeft(df)((d,t) => catalyst(t,df,count))  
}
