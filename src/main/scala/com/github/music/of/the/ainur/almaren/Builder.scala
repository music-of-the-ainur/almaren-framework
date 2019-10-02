package com.github.music.of.the.ainur.almaren

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.core._
import zipper._

private[almaren] trait Builder extends LazyLogging with Serializable {

  var tree:Option[Tree] = None

  def sourceSql(sql: String):State = {
    new SourceSql(sql)
  }

  private[almaren] def build(state: State): Tree = {
    tree match {
      case Some(t) => addTree(t,state) // Complex build logic will be here
      case None => Tree(state)
    }
  }

  private def addTree(tree: Tree, state: State): Tree = {
      Zipper(tree)
        .tryAdvanceRightDepthFirst.orStay
        .tryAdvanceLeftDepthFirst.orStay
        .insertLeft(Tree(state)).commit
  }

}
