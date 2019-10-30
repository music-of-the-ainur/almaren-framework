package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Container
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.{TargetJdbc, TargetSql}
import org.apache.spark.sql.SaveMode

private[almaren] trait Target extends Core {
  def targetSql(sql: String): Option[Container] = 
    new TargetSql(sql: String)

  def targetJdbc(url: String, driver: String, query: String, saveMode:SaveMode = SaveMode.ErrorIfExists, params:Map[String,String] = Map[String,String]()): Option[Container] = 
    new TargetJdbc(url,driver,query,saveMode,params)
}