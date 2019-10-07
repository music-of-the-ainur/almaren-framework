package com.github.music.of.the.ainur.almaren.component

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import zipper.Zipper.MoveResult.Success
import zipper._

private[almaren] object Builder extends LazyLogging with Serializable {

  def addLeft(state: State, tree: Tree): Tree =
    depthLeft(Zipper(tree)).insertDownLeft(List(Tree(state))).commit

  private def depthLeft(zipper: Zipper[Tree]): Zipper[Tree] =
    zipper.tryMoveDownLeft match {
      case Zipper.MoveResult.Success(s, _) => depthLeft(s)
      case Zipper.MoveResult.Failure(f) => f
   }

  def addRight(tree: Tree, insertTree: List[Tree]): Tree =
    Zipper(tree).advanceLeftDepthFirst.moveDownLeft.insertDownRight(insertTree).commit
}
