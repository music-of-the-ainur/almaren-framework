package com.github.music.of.the.ainur.almaren.component

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import zipper._

private[almaren] object Builder extends LazyLogging with Serializable {
  def addLeft(state: State, zipper: Zipper[Tree]): Zipper[Tree] =
    zipper.tryAdvanceRightDepthFirst.orStay.insertDownLeft(List(Tree(state)))

  def addRight(zipper: Zipper[Tree], insertTree: List[Tree]): Zipper[Tree] =
    zipper.insertDownRight(insertTree)
}
