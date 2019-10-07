package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.Tree
import com.github.music.of.the.ainur.almaren.component.state.core.{SourceJdbc, SourceSql}
import com.github.music.of.the.ainur.almaren.component.Implicit._

private[ainur] trait Source {
  def sourceSql(sql: String): Tree =
    new SourceSql(sql)

  def sourceJdbc(url: String, driver: String, query: String, params:Map[String,String] = Map[String,String]()): Tree =
    new SourceJdbc(url, driver, query, params)
}
