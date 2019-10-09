package com.github.music.of.the.ainur.almaren.component

sealed trait AlmarenException {
  self: Throwable =>
}

case class NullCatalyst() 
    extends Exception(s"Can't execute catalyst with an empty Container") with AlmarenException

case class NullFork() 
    extends Exception(s"Can't execute fork with an empty Container") with AlmarenException
