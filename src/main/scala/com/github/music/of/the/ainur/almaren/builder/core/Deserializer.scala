package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core._
import com.github.music.of.the.ainur.almaren.{Tree, InvalidDecoder, SchemaRequired, State}

private[almaren] trait Deserializer extends Core {
  def deserializer(decoder:String,columnName:String,schemaInfo:Option[String] = None,params:Map[String,String]=Map()): Option[Tree] = {

    def json(): State =
      JsonDeserializer(columnName,schemaInfo,params)
    def xml(): State =
      XMLDeserializer(columnName,params)
    def avro(): State =
      AvroDeserializer(columnName,schemaInfo.getOrElse(throw SchemaRequired(decoder)),params)


    decoder.toUpperCase match {
      case "JSON" => json
      case "XML" => xml
      case "AVRO" => avro
      case d => throw InvalidDecoder(d)
    }
  }
}
