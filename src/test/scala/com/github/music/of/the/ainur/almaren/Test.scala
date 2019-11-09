package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.state.core.{Cache, SourceSql, Sql, TargetSql}
import org.apache.spark.sql.SaveMode
import org.scalatest._

import scala.collection.immutable._


class Test extends FunSuite with BeforeAndAfter {
  val almaren = Almaren("App Test")

  val spark = almaren.spark
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()


  val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/movies.json")).mkString
  import spark.implicits._

  val res = spark.read.json(Seq(json_str).toDS)
  res.createTempView("movies")


  val movies = almaren.builder
    .sourceSql("select monotonically_increasing_id() as id,* from movies")
    .sql("select * from __table__")
    .fork(
      almaren.builder.sql("""select id,title from __TABLE__""").alias("title"),
      almaren.builder.sql("""select id,year from __TABLE__""").alias("year")
    ).dsl("""
		|title$title:StringType
		|year$year:LongType
		|cast[0]$actor:StringType
		|cast[1]$support_actor:StringType
 		|genres[0]$genre:StringType""".stripMargin)
    .sql("""SELECT * FROM __TABLE__""")
    .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)
    .alias("foo")


  val df = almaren.batch(movies)
  df.show(false)

  spark.sql("select * from title").show(false)

  after {
    spark.stop()
  }
}
