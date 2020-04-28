package com.github.music.of.the.ainur.almaren.builder.core

import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.{SourceJdbc, SourceSql}

private[almaren] trait Source extends Core {
  def sourceSql(sql: String): Option[Tree] =
    SourceSql(sql)

  def sourceJdbc(url: String, driver: String,query: String,user : Option[String],password : Option[String],params:Map[String,String] = Map[String,String]()): Option[Tree] =
    SourceJdbc(url, driver, query,user,password,params)
}
