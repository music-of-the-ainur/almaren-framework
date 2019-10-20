package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.state.core.{Cache, SourceSql, Sql, TargetSql}
import org.scalatest._

import scala.collection.immutable._


class Test extends FunSuite with BeforeAndAfter {
  val almaren = Almaren("App Test")
  val spark = almaren.spark.master("local[*]").getOrCreate()
  System.setSecurityManager(null)


  val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/movies.json")).mkString
  import spark.implicits._

  val res = spark.read.json(Seq(json_str).toDS)
  res.createTempView("movies")

/*
  val tree = Tree(
    new SourceSql("select monotonically_increasing_id() as id,* from movies"),
    List(Tree(new Cache(true,None),
      List(
        Tree(new Sql("select year from __TABLE__"),
          List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS year SELECT distinct year FROM __TABLE__")))),
        Tree(new Sql("select id, title from __TABLE__"),
          List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS title SELECT * FROM __TABLE__")))),
        Tree(new Sql("select genres from __TABLE__"),
          List(Tree(new Sql("select genres,count(*) as total from (select explode_outer(genres) as genres from __TABLE__) G where genres is not null group by genres"),
            List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS genres SELECT * FROM __TABLE__")))))),
        Tree(new Sql("select cast from __TABLE__ where year >= 1990"),
          List(Tree(new TargetSql("CREATE TABLE IF NOT EXISTS cast SELECT cast,count(*) as total FROM (SELECT explode_outer(cast) as cast FROM __TABLE__) C where cast is not null and cast != 'and' group by cast")))
        ),
      )
    ))
  )



  val foo = almaren.builder.sourceSql("select monotonically_increasing_id() as id,* from movies").sql("select * from __TABLE__").cache().fork(
    almaren.builder
      .sql("select year from __TABLE__")
      .targetSql("CREATE TABLE IF NOT EXISTS year SELECT distinct year FROM __TABLE__"),
    almaren.builder
      .sql("select id, title from __TABLE__")
      .targetSql("CREATE TABLE IF NOT EXISTS title SELECT * FROM __TABLE__"),
    almaren.builder
      .sql("select genres from __TABLE__")
      .sql("select genres,count(*) as total from (select explode_outer(genres) as genres from __TABLE__) G where genres is not null group by genres")
      .targetSql("CREATE TABLE IF NOT EXISTS genres SELECT * FROM __TABLE__"),
    almaren.builder
      .sql("select cast from __TABLE__ where year >= 1990")
      .targetSql("CREATE TABLE IF NOT EXISTS cast SELECT cast,count(*) as total FROM (SELECT explode_outer(cast) as cast FROM __TABLE__) C where cast is not null and cast != 'and' group by cast")
  ).coalesce(10)
 


  println(foo.get.zipper.commit)

  almaren.catalyst(foo).show(false)


  spark.sql("select * from year").show(false)
  spark.sql("select * from title").show(false)
  spark.sql("select * from genres order by total desc").show(false)
  spark.sql("select * from cast order by total desc").show(false)

 */

  val movies = almaren.builder
    .sourceSql("select monotonically_increasing_id() as id,* from movies where year >= 2000")
    .dsl("""
        |id$id:LongType
		|title$title:StringType
		|year$year:LongType
		|cast@cast
		|	cast$cast:StringType
 		|genres@genre
		|	genre$genre:StringType""".stripMargin)
  .sql("""SELECT sha2(concat_ws("",array(title,year,cast,genre)),256) as unique_hash,* FROM __TABLE__ WHERE cast <> "(voice)" and cast <> "(Narrator)" order by title""")

  val df = almaren.catalyst(movies)
  df.show(false)
  df.printSchema()
  df.describe()

  spark.stop()

}
