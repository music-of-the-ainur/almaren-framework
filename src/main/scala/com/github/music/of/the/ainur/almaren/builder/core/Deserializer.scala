package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Container
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core._
import com.github.music.of.the.ainur.almaren.{State,InvalidDecoder,SchemaRequired}

private[almaren] trait Deserializer extends Core {
  def deserializer(decoder:String,columnName:String,schemaInfo:Option[String] = None): Option[Container] = {

    def json(): State =
      new JsonDeserializer(columnName,schemaInfo.getOrElse(throw SchemaRequired(decoder)))
    def xml(): State =
      new XMLDeserializer(columnName)
    def avro(): State =
      new AvroDeserializer(columnName,schemaInfo.getOrElse(throw SchemaRequired(decoder)))


    decoder.toUpperCase match {
      case "JSON" => json
      case "XML" => xml
      case "AVRO" => avro
      case d => throw InvalidDecoder(d)
    }
  }
}
