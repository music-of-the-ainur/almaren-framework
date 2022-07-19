package com.github.music.of.the.ainur.almaren.state.core

import com.github.music.of.the.ainur.almaren.{Almaren, SchemaRequired, State}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructType}

import scala.language.implicitConversions
import com.github.music.of.the.ainur.almaren.util.Constants
import org.apache.spark.sql.Dataset

trait Deserializer extends State {

  def columnName: String

  def schema: Option[String]

  def options: Map[String, String]

  def autoFlatten: Boolean

  override def executor(df: DataFrame): DataFrame = deserializer(df)

  def deserializer(df: DataFrame): DataFrame

  implicit def string2Schema(schema: String): StructType =
    StructType.fromDDL(schema)

  def autoFlatten(df: DataFrame, columnName: String): DataFrame =
    df.
      select("*", columnName.concat(".*")).
      drop(columnName)

  def sampleData[T](df: Dataset[T]): Dataset[T] =
    df.sample(
      options.get("samplingRatio").orElse(Some("1.0")).get.toDouble
    )
}

case class AvroDeserializer(columnName: String, schema: Option[String], options: Map[String, String], autoFlatten: Boolean) extends Deserializer {

  import org.apache.spark.sql.avro._
  import org.apache.spark.sql.functions._
  import collection.JavaConversions._

  override def deserializer(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    val newDf = df.withColumn(columnName,
      from_avro(
        col(columnName),
        schema.get)
    )
    if (autoFlatten)
      autoFlatten(newDf, columnName)
    else
      newDf
  }
}

case class JsonDeserializer(columnName: String, schema: Option[String], options: Map[String, String], autoFlatten: Boolean) extends Deserializer {

  import org.apache.spark.sql.functions._
  import collection.JavaConversions._

  override def deserializer(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    val newDf = df.withColumn(columnName,
      from_json(
        col(columnName),
        schema.getOrElse(getSchemaDDL(df.selectExpr(columnName).as[(String)], options)),
        options
      ))
    if (autoFlatten)
      autoFlatten(newDf, columnName)
    else
      newDf
  }

  private def getSchemaDDL(df: Dataset[String], options: Map[String, String]): String =
    Almaren.spark.getOrCreate().read.options(options).json(sampleData(df)).schema.toDDL
}

case class XMLDeserializer(columnName: String, schema: Option[String], options: Map[String, String], autoFlatten: Boolean) extends Deserializer {

  import org.apache.spark.sql.functions._
  import com.databricks.spark.xml.functions.from_xml
  import com.databricks.spark.xml.schema_of_xml
  import collection.JavaConversions._

  override def deserializer(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    val xmlSchema = schema match {
      case Some(s) => StructType.fromDDL(s)
      case None => schema_of_xml(df.select(columnName).as[String],options)
    }
    println(xmlSchema)
    val newDf = df.withColumn(columnName,
      from_xml(col(columnName),
        xmlSchema,
        options
      ))
    if (autoFlatten)
      autoFlatten(newDf, columnName)
    else
      newDf
  }
}
