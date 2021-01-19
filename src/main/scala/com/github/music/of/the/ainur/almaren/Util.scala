package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame

object Util {
  val spark = Almaren.spark.getOrCreate()

  import spark.implicits._

  def genDDLFromJsonString(df: DataFrame, field: String): String = {

    spark.read.json(df.select(field).as[String]).sample(0.1).schema.toDDL
  }

  def genDDLFromDataFrame(df: DataFrame): String = {
    df.sample(0.1).schema.toDDL
  }
}
