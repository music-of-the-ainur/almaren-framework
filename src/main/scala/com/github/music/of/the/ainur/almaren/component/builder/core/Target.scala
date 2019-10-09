package com.github.music.of.the.ainur.almaren.component.builder.core

import com.github.music.of.the.ainur.almaren.component.{Builder, State, Container}
import com.github.music.of.the.ainur.almaren.component.builder.Core
import com.github.music.of.the.ainur.almaren.component.state.core._

private[almaren] trait Target extends Core {
  def targetSql(sql: String): Option[Container] = 
    new TargetSql(sql: String)
}
