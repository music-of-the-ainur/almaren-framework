package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.Tree
import com.github.music.of.the.ainur.almaren.component.state.core.{SourceJdbc, SourceSql}
import com.github.music.of.the.ainur.almaren.component.builder.Core

private[almaren] trait Source extends Core {
  def sourceSql(sql: String): Option[Tree] =
    new SourceSql(sql)

  def sourceJdbc(url: String, driver: String, query: String, params:Map[String,String] = Map[String,String]()): Option[Tree] =
    new SourceJdbc(url, driver, query, params)
}
