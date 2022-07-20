package com.github.music.of.the.ainur.almaren.state.core

import com.github.music.of.the.ainur.almaren.{Almaren, SchemaRequired, State}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Dataset}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.language.implicitConversions
import com.github.music.of.the.ainur.almaren.util.Constants
import org.apache.spark.sql.functions.col

import javax.xml.crypto.Data

trait Deserializer extends State {

  def columnName: String
  def schema: Option[String]
  def options: Map[String, String]
  def autoFlatten: Boolean

  override def executor(df: DataFrame): DataFrame = {
    val newDf = deserializer(df)
    if(autoFlatten)
      autoFlatten(newDf,columnName)
    else
      newDf
  }

  def deserializer(df: DataFrame): DataFrame

  implicit def string2Schema(schema: String): StructType =
    StructType.fromDDL(schema)

  def autoFlatten(df: DataFrame, columnName: String): DataFrame =
    df.
      select("*", columnName.concat(".*")).
      drop(columnName)

  def sampleData[T](df: Dataset[T]): Dataset[T] = {
    df.sample(
      options.getOrElse("samplingRatio","1.0").toDouble
    ).limit(
      options.getOrElse("samplingMaxLines","10000").toInt
    )
  }

  def getReadWithOptions: DataFrameReader =
    Almaren.spark.getOrCreate().read.options(options)

  def getDDL(df:DataFrame): String =
    df.schema.toDDL
}

case class AvroDeserializer(columnName: String, schema: Option[String] = None, options: Map[String, String], autoFlatten: Boolean, mandatorySchema: String) extends Deserializer {

  import org.apache.spark.sql.avro.functions.from_avro
  import org.apache.spark.sql.functions._
  import collection.JavaConversions._

  schema.map(_ => throw SchemaRequired(s"AvroDeserializer, don't use 'schema' it must be None, use 'mandatorySchema' "))

  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}, schema:{$mandatorySchema}, options:{$options}, autoFlatten:{$autoFlatten}")
    df.withColumn(columnName, from_avro(col(columnName), mandatorySchema, options))
  }
}

case class JsonDeserializer(columnName: String, schema: Option[String], options: Map[String, String], autoFlatten: Boolean) extends Deserializer {

  import org.apache.spark.sql.functions._
  import collection.JavaConversions._

  override def deserializer(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"columnName:{$columnName}, schema:{$schema}, options:{$options}, autoFlatten:{$autoFlatten}")
    df.withColumn(columnName,
      from_json(
        col(columnName),
        schema.getOrElse(getSchemaDDL(df.selectExpr(columnName).as[(String)])),
        options
      ))
  }

  private def getSchemaDDL(df: Dataset[String]): String =
    getDDL(getReadWithOptions.json(sampleData(df)))
}

case class XMLDeserializer(columnName: String, schema: Option[String], options: Map[String, String], autoFlatten: Boolean) extends Deserializer {

  import com.databricks.spark.xml.functions.from_xml
  import com.databricks.spark.xml.schema_of_xml
  import org.apache.spark.sql.functions._

  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}, schema:{$schema}, options:{$options}, autoFlatten:{$autoFlatten}")
    import df.sparkSession.implicits._
    val xmlSchema = schema match {
      case Some(s) => StructType.fromDDL(s)
      case None => schema_of_xml(sampleData(df.select(columnName).as[String]), options = options)
    }
    df
      .withColumn(columnName, from_xml(col(columnName), xmlSchema, options))
  }
}

case class CSVDeserializer(columnName: String, schema: Option[String], options: Map[String, String], autoFlatten: Boolean) extends Deserializer {

  import org.apache.spark.sql.functions._
  import collection.JavaConversions._

  override def deserializer(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"columnName:{$columnName}, schema:{$schema}, options:{$options}, autoFlatten:{$autoFlatten}")
    df.withColumn(columnName,
      from_csv(
        col(columnName),
        schema.getOrElse(getSchemaDDL(df.selectExpr(columnName).as[(String)])),
        options
      ))
  }

  private def getSchemaDDL(df: Dataset[String]): String =
    getDDL(getReadWithOptions.csv(sampleData(df)))
}