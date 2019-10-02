package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.core._

class Test {
  val almaren = Almaren("App Test")


/*
  val node = Tree(
    new SourceSql("select * from foo"),List(
    Node(new JsonDeserializer("foo","bar")
      Node(new SQLState("select 1")),Node(new SQLState("select 2"))
    )))

 */
  val tree = Tree(
    new SourceSql("select * from foo"),
    List(Tree(new JsonDeserializer("foo","bar"),
      List(
        Tree(new SQLState("select 1")),
        Tree(new SQLState("select 2")),
      ))
    ))

  println {
    almaren.catalyst(tree)
  }


}
