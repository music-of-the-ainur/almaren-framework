package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.core._
import org.apache.spark.sql.types.StructType

class Test {
  val almaren = Almaren("App Test")


  val node = Node(
    new SourceSql("select * from foo"),
    Node(new JsonDeserializer("foo","bar"),
      Node(new SQLState("select 1")),Node(new SQLState("select 2"))
    ))
}
