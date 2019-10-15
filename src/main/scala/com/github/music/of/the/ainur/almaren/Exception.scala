package com.github.music.of.the.ainur.almaren

sealed trait AlmarenException {
  self: Throwable =>
}

case class NullCatalyst() 
    extends Exception(s"Can't execute catalyst with an empty Container") with AlmarenException

case class NullFork() 
    extends Exception(s"Can't execute fork with an empty Container") with AlmarenException

case class InvalidDecoder(decoder: String) 
    extends Exception(s"Invalid decoder:{$decoder}") with AlmarenException

case class SchemaRequired(decoder: String) 
    extends Exception(s"Schema is mandatory for the decoder:{$decoder}") with AlmarenException
