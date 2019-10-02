package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.core._
import org.scalatest._

class Test extends FunSuite with BeforeAndAfter {
  val almaren = Almaren("App Test")
  val spark = almaren.spark.master("local[*]").getOrCreate()

  val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/movies.json")).mkString
  import spark.implicits._

  val res = spark.read.json(Seq(json_str).toDS)
  res.show
  res.printSchema()
  res.createTempView("movies")

  val tree = Tree(
    new SourceSql("select * from movies"),
    List(Tree(new Cache(true,None),
      List(
        Tree(new SQLState("select year from __TABLE__")),
        Tree(new SQLState("select title from __TABLE__")),
        Tree(new SQLState("select genres from __TABLE__"), 
          List(Tree(new SQLState("select genres,count(*) from (select explode_outer(genres) as genres from __TABLE__) G where genres is not null group by genres order by count(*) desc")))),
        Tree(new SQLState("select cast from __TABLE__")),
      )
    ))
  )

  println {
    almaren.catalyst(tree).show(false)
  }
}
