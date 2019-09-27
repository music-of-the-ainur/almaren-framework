package com.github.music.of.the.ainur.almaren.core

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame

private[almaren] trait Builder extends LazyLogging with Serializable {

  def component[Node](node: Node): Node

}
