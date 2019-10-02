package com.github.music.of.the.ainur.almaren.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}
import scala.language.implicitConversions
import com.github.music.of.the.ainur.almaren.State

abstract class Deserializer() extends State {
  override def executor(df: DataFrame): DataFrame = deserializer(df)
  def deserializer(df: DataFrame): DataFrame
  implicit def string2Schema(schema: String): DataType =
    StructType.fromDDL(schema)
}

class AvroDeserializer(columnName: String,schema: String) extends Deserializer {
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.avro._
  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    df.withColumn(columnName,from_avro(col(columnName),schema))
      .select("*",columnName.concat(".*")).drop(columnName)
  }
}

class JsonDeserializer(columnName: String,schema: String) extends Deserializer {
  import org.apache.spark.sql.functions._
  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    df.withColumn(columnName,from_json(col(columnName),schema))
      .select("*",columnName.concat(".*")).drop(columnName)
  }
}

class XMLDeserializer(columnName: String) extends Deserializer {
  import com.databricks.spark.xml.XmlReader
  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}")
    new XmlReader().xmlRdd(df.sparkSession,df.select(columnName).rdd.map(r => r(0).asInstanceOf[String])).toDF
  }
}
