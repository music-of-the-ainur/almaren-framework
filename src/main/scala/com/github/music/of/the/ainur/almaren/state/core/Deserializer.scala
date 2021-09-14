package com.github.music.of.the.ainur.almaren.state.core

import com.github.music.of.the.ainur.almaren.State
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

import scala.language.implicitConversions
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.util.Constants
import org.apache.spark.sql.Dataset

abstract class Deserializer() extends State {
  override def executor(df: DataFrame): DataFrame = deserializer(df)
  def deserializer(df: DataFrame): DataFrame
  implicit def string2Schema(schema: String): DataType =
    StructType.fromDDL(schema)
}

case class AvroDeserializer(columnName: String,schema: String) extends Deserializer {
  import org.apache.spark.sql.avro._
  import org.apache.spark.sql.functions._
  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    df.withColumn(columnName,from_avro(col(columnName),schema))
      .select("*",columnName.concat(".*")).drop(columnName)
  }
}

case class JsonDeserializer(columnName: String,schema: Option[String]) extends Deserializer {
  import org.apache.spark.sql.functions._
  override def deserializer(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    df.withColumn(columnName,
      from_json(col(columnName),
        schema.getOrElse(getSchemaDDL(df.selectExpr(columnName).as[(String)]))))
      .select("*",columnName.concat(".*"))
      .drop(columnName)
  }
  private def getSchemaDDL(df: Dataset[String]): String =
    Almaren.spark.getOrCreate().read.json(df.sample(Constants.sampleDeserializer)).schema.toDDL
}

case class XMLDeserializer(columnName: String, schema: Option[String]) extends Deserializer {
  import com.databricks.spark.xml.functions.from_xml
  import com.databricks.spark.xml.schema_of_xml
  import org.apache.spark.sql.functions._

  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}")
    import df.sparkSession.implicits._
    val xmlSchema = schema match {
      case Some(s) => StructType.fromDDL(s)
      case None => schema_of_xml(df.select(columnName).as[String])
    }
    df
      .withColumn(columnName, from_xml(col(columnName), xmlSchema))
      .select("*",columnName.concat(".*"))
      .drop(columnName)
  }
}
