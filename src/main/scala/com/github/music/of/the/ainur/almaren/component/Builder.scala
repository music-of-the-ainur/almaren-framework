package com.github.music.of.the.ainur.almaren.component

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import zipper._

private[almaren] object Builder extends LazyLogging with Serializable {

  def addLeft(state: State,tree: Tree): Tree = 
     Zipper(tree)
          .tryAdvanceRightDepthFirst.orStay
          .tryAdvanceLeftDepthFirst.orStay
          .insertLeft(Tree(state)).commit
}
