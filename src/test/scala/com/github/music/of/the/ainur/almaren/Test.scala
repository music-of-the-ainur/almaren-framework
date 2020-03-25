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


  val testTable = "movies"

  import spark.implicits._

  createSampleData(testTable)

  // TODO improve it
  val movies = almaren.builder
    .sourceSql(s"select monotonically_increasing_id() as id,* from $testTable")
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

  test(readTest("foo_table"), movies, "foo")
  test(readTest("title_table"), spark.sql("select * from title"), "title")
  test(readTest("year_table"), spark.sql("select * from year"), "year")

  val moviesDf = spark.table(testTable)

  test(testSourceTargetJdbc(moviesDf), moviesDf, "SourceTargetJdbcTest")
  repartitionAndColaeseTest(moviesDf)
  aliasTest(moviesDf)
  
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

  def createSampleData(tableName: String): Unit = {
    val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/movies.json")).mkString
    val res = spark.read.json(Seq(json_str).toDS)
    res.createTempView(tableName)
  }


  def testSourceTargetJdbc(df: DataFrame): DataFrame = {
    almaren.builder
      .sourceSql(s"select * from $testTable")
      .targetJdbc("jdbc:postgresql://localhost/almaren", "org.postgresql.Driver", "movies_test", SaveMode.Overwrite)
      .batch

    almaren.builder
      .sourceJdbc("jdbc:postgresql://localhost/almaren", "org.postgresql.Driver", "select * from movies_test")
      .batch
  }

  def repartitionAndColaeseTest(dataFrame: DataFrame) {
    val repartition_df = almaren.builder.sourceSql(s"select * from $testTable")
      .repartition(10).batch

    repartition_df.createTempView("test_new")
    val coalase_df=almaren.builder.sourceSql("select * from test_new")
      .coalesce(5).batch

    val repartition_size=repartition_df.rdd.partitions.size
    val coalese_size=coalase_df.rdd.partitions.size

    test("repartition") {
      assert(repartition_size == 10)
    }
    test("coalesce"){
      assert(coalese_size == 5)
    }

  }
  def aliasTest(df:DataFrame): Unit ={
    almaren.builder.sourceSql(s"select * from $testTable").alias("alias_test").batch

    val aliasTableDf=spark.read.table("alias_test")
    val aliasTableCount=aliasTableDf.count()

    test("alias")
    {
      assert(aliasTableCount>0)
    }
  }
}
