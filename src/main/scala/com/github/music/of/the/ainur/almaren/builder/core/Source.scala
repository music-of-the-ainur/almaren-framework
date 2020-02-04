package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Container
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.{SourceJdbc, SourceSql}

private[almaren] trait Source extends Core {
  def sourceSql(sql: String): List[Container] =
    new SourceSql(sql)

  def sourceJdbc(url: String, driver: String, query: String, params:Map[String,String] = Map[String,String]()): List[Container] =
    new SourceJdbc(url, driver, query, params)
}
