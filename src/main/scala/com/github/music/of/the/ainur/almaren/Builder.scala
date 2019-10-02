package com.github.music.of.the.ainur.almaren

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.core._

private[almaren] trait Builder extends LazyLogging with Serializable {

  var tree:Option[Tree] = None

  def sourceSql(sql: String):State = {
    new SourceSql(sql)
  }

  private[almaren] def build(state: State): Option[Tree] = {
    tree match {
      case Some(t) => t // Complex build logic will be here
      case None => tree = Some(Tree(state)) 
    }
    tree
  }

}
