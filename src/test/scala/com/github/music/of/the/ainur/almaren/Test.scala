package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.core._
import org.scalatest._

class Test extends FunSuite with BeforeAndAfter {
  val almaren = Almaren("App Test")
  val spark = almaren.spark.master("local[*]").enableHiveSupport.getOrCreate()
  System.setSecurityManager(null)


  val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/movies.json")).mkString
  import spark.implicits._

  val res = spark.read.json(Seq(json_str).toDS)
 
 res.show
  res.printSchema()
  res.createTempView("movies")

  val tree = Tree(
    new SourceSql("select monotonically_increasing_id() as id,* from movies"),
    List(Tree(new Cache(true,None),
      List(
        Tree(new SQLState("select year from __TABLE__"),
          List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS year SELECT distinct year FROM __TABLE__")))),
        Tree(new SQLState("select id, title from __TABLE__"),
          List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS title SELECT * FROM __TABLE__")))),
        Tree(new SQLState("select genres from __TABLE__"),
          List(Tree(new SQLState("select genres,count(*) as total from (select explode_outer(genres) as genres from __TABLE__) G where genres is not null group by genres"),
            List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS genres SELECT * FROM __TABLE__")))))),
        Tree(new SQLState("select cast from __TABLE__ where year >= 1990"),
          List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS cast SELECT cast,count(*) as total FROM (SELECT explode_outer(cast) as cast FROM __TABLE__) C where cast is not null and cast != 'and' group by cast")))
        ),
      )
    ))
  )

  almaren.catalyst(tree) 

  spark.sql("select * from year").show(false)
  spark.sql("select * from title").show(false)
  spark.sql("select * from genres order by total desc").show(false)
  spark.sql("select * from cast order by total desc").show(false)
  
  spark.stop()
}
