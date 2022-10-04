package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core._
import com.github.music.of.the.ainur.almaren.{InvalidDecoder, SchemaRequired, State, Tree}

import java.util.Collections

private[almaren] trait Deserializer extends Core {
  def deserializer(decoder:String,columnName:String,schemaInfo:Option[String] = None,options:Map[String,String] = Map(),autoFlatten:Boolean = true): Option[Tree] = {

    def json: State =
      JsonDeserializer(columnName,schemaInfo,options,autoFlatten)
    def xml: State =
      XMLDeserializer(columnName,schemaInfo,options,autoFlatten)
    def avro: State =
      AvroDeserializer(columnName,None,options,autoFlatten,schemaInfo.getOrElse(throw SchemaRequired(decoder)))
    def csv: State =
      CSVDeserializer(columnName, schemaInfo, options, autoFlatten)

    decoder.toUpperCase match {
      case "JSON" => json
      case "XML" => xml
      case "AVRO" => avro
      case "CSV" => csv
      case d => throw InvalidDecoder(d)
    }
  }
}