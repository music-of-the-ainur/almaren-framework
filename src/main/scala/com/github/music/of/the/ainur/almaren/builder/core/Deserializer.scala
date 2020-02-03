package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core._
import com.github.music.of.the.ainur.almaren.{Container, InvalidDecoder, SchemaRequired, State}

private[almaren] trait Deserializer extends Core {
  def deserializer(decoder:String,columnName:String,schemaInfo:Option[String] = None): Option[List[Container]] = {

    def json(): State =
      new JsonDeserializer(columnName,schemaInfo.getOrElse(throw SchemaRequired(decoder)))
    def xml(): State =
      new XMLDeserializer(columnName)

    decoder.toUpperCase match {
      case "JSON" => json
      case "XML" => xml
      case d => throw InvalidDecoder(d)
    }
  }
}
