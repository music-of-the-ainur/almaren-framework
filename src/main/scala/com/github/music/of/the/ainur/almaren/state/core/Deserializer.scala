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

case class JsonDeserializer(columnName: String,schema: Option[String]) extends Deserializer {
  import org.apache.spark.sql.functions._
  override def deserializer(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    logger.info(s"columnName:{$columnName}, schema:{$schema}")
    df.withColumn(columnName,
      from_json(col(columnName),
        schema.getOrElse(schemaGen(df.selectExpr(columnName).as[(String)]))))
      .select("*",columnName.concat(".*"))
      .drop(columnName)
  }

  private def inferSchema(schema: String): StructType = 
    DataType.fromJson(schema).asInstanceOf[StructType]

  private def schemaGen(ds: Dataset[String]): String =
    Almaren.spark.getOrCreate().read.json(ds.sample(true, Constants.sampleDeserializer)).schema.json

}

case class XMLDeserializer(columnName: String) extends Deserializer {
  import com.databricks.spark.xml.XmlReader
  override def deserializer(df: DataFrame): DataFrame = {
    logger.info(s"columnName:{$columnName}")
    new XmlReader().xmlRdd(df.sparkSession,df.select(columnName).rdd.map(r => r(0).asInstanceOf[String])).toDF
  }
}
