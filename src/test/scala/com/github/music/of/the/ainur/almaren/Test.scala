package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode}
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
    ).dsl(
    """
		|title$title:StringType
		|year$year:LongType
		|cast[0]$actor:StringType
		|cast[1]$support_actor:StringType
 		|genres[0]$genre:StringType""".stripMargin)
    .sql("""SELECT * FROM __TABLE__""")
    .batch




  val movies_df=movies.limit(10)

  test(readTest("foo_table"), movies, "foo")
  test(readTest("title_table"), spark.sql("select * from title"), "title")
  test(readTest("year_table"), spark.sql("select * from year"), "year")
  //test(testSourceJdbc(movies_df), movies_df, "SourceJdbcTest")
  repartitionAndColaeseTest(movies)
  aliasTest(movies_df)
  cacheTest(movies_df)
  testingPipe(movies_df)


  after {
    spark.stop()
  }


  def test(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    testCount(df1, df2, name)
    testCompare(df1, df2, name)
  }

  def testCount(df1: DataFrame, df2: DataFrame, name: String): Unit = {
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
  def testCompare(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val diff = compare(df1, df2)
    test(s"Compare Test:$name should be zero") {
      assert(diff == 0)
    }
    test(s"Compare Test:$name, should not be able to join") {
      assertThrows[AnalysisException] {
        compare(df2, spark.emptyDataFrame)
      }
    }
  }

  private def compare(df1: DataFrame, df2: DataFrame): Long =
    df1.as("df1").join(df2.as("df2"), joinExpression(df1), "leftanti").count()

  private def joinExpression(df1: DataFrame): Column =
    df1.schema.fields
      .map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}"))
      .reduce((col1, col2) => col1.and(col2))

  def readTest(file: String): DataFrame =
    spark.read.parquet(s"src/test/resources/sample_output/$file.parquet")

  def writeTest(df: DataFrame, file: String): Unit =
    df.write.parquet(s"src/test/resources/sample_output/$file.parquet")

  def createSampleData: Unit = {
    val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/movies.json")).mkString
    val res = spark.read.json(Seq(json_str).toDS)
    res.createTempView("movies")
  }


  def testSourceJdbc(df: DataFrame): DataFrame = {
    df.createTempView("movies_test")

    val target_jdbc_df: DataFrame = almaren.builder
      .sourceSql("select * from movies_test")
      .targetJdbc("jdbc:postgresql://localhost/almaren?user=postgres", "org.postgresql.Driver", "movies_test", SaveMode.Overwrite)
      .batch

    val source_jdbc_df = almaren.builder
      .sourceJdbc("jdbc:postgresql://localhost/almaren?user=postgres", "org.postgresql.Driver", "select * from movies_test")
      .batch


    source_jdbc_df
  }

  def repartitionAndColaeseTest(dataFrame: DataFrame) {
    val df=dataFrame.limit(200)
    df.createTempView("test")

    val repartition_df = almaren.builder.sourceSql("select * from test")
      .repartition(10).batch

    repartition_df.createTempView("test_new")
    val coalase_df=almaren.builder.sourceSql("select * from test_new")
      .coalesce(5).batch

    val repartition_size=repartition_df.rdd.partitions.size
    val coalese_size=coalase_df.rdd.partitions.size

    test("Test for Repartition Size") {
      assert(repartition_size == 10)
    }
    test("Test for coalesce size"){
      assert(coalese_size == 5)
    }

  }
  def aliasTest(df:DataFrame): Unit ={

    df.createTempView("Alias_table")

    val table_df= almaren.builder.sourceSql("select * from Alias_table").alias("alias_test").batch

  val alias_table_df=spark.read.table("alias_test")
    val alias_table_count=alias_table_df.count()

    test("alias test")
    {
      assert(alias_table_count>0)
    }
  }


def cacheTest(df:DataFrame): Unit ={

  df.createTempView("cache_test")

  val testCacheDf:DataFrame = almaren.builder.sourceSql("select * from cache_test").cache(true).batch
  val bool_cache=testCacheDf.storageLevel.useMemory
  test("Testing cache")
  {
    assert(bool_cache)
  }

  val testUnCacheDf=almaren.builder.sourceSql("select * from cache_test").cache(false).batch
  val bool_uncache=testUnCacheDf.storageLevel.useMemory
  test("testing uncache")
  {
    assert(!bool_uncache)
  }

}

  def testingPipe(df:DataFrame): Unit ={
    df.createTempView("pipe_view")
    val pipeRDD=almaren.builder.sql("select * from pipe_view").pipe("src/test/resources/echo.sh").batch
    val pipeRddCount=pipeRDD.count()
    
    test("testing pipe")
    {
      assert(pipeRddCount>0)
    }
  }

}




