package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.scalatest._

import scala.collection.immutable._

class Test extends FunSuite with BeforeAndAfter {
  val almaren = Almaren("App Test")

  val spark = almaren.spark
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  import spark.implicits._

  createSampleData
  
  // TODO improve it
  // TODO Add jdbc test
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

  test(readTest("foo_table"),almaren.batch(movies),"foo")
  test(readTest("title_table"),spark.sql("select * from title"),"title")
  test(readTest("year_table"),spark.sql("select * from year"),"year")

  after {
    spark.stop()
  }


  def test(df1:DataFrame,df2:DataFrame,name:String): Unit = {
    testCount(df1,df2,name)
    testCompare(df1,df2,name)
  }

  def testCount(df1:DataFrame,df2:DataFrame,name:String): Unit = {
    val count1 = df1.count()
    val count2 = df2.count()
    val count3 = spark.emptyDataFrame.count()
    test(s"Count Test:$name should match") {
      assert(count1 == count2)
    }
    test(s"Count Test:$name should not match") {
      assert(count1 != count3)
    }
  }

  // Doesn't support nested type and we don't need it :)
  def testCompare(df1:DataFrame,df2:DataFrame,name:String): Unit = {
    val diff = df1.as("df1").join(df2.as("df2"),joinExpression(df1),"leftanti").count()
    test(s"Compare Test:$name should be zero") {
      assert(diff == 0)
    }
  }

  private def joinExpression(df1:DataFrame): Column = {
    val columns = df1.schema.fields.map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}") )
    columns.tail.foldLeft(columns.head)((col1,col2) => col1.and(col2))
  }

  def readTest(file: String):DataFrame = 
    spark.read.parquet(s"src/test/resources/sample_output/$file.parquet")

  def writeTest(df:DataFrame,file: String):Unit = 
    df.write.parquet(s"src/test/resources/sample_output/$file.parquet")

  def createSampleData: Unit = {
    val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/movies.json")).mkString
    val res = spark.read.json(Seq(json_str).toDS)
    res.createTempView("movies")
  }

}
