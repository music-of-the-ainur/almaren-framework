package com.github.music.of.the.ainur.almaren

import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode}
import org.scalatest._
import org.apache.spark.sql.avro._



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
  test(testSourceTargetJdbcUserPassword(moviesDf), moviesDf, "SourceTargetJdbcTestUserPassword")
  test(testSourceFile("parquet","src/test/resources/sample_data/emp.parquet"),
    spark.read.parquet("src/test/resources/sample_output/employee.parquet"),"SourceParquetFileTest")
  test(testSourceFile("avro","src/test/resources/sample_data/emp.avro"),
    spark.read.parquet("src/test/resources/sample_output/employee.parquet"),"SourceAvroFileTest")
  repartitionAndColaeseTest(moviesDf)
  aliasTest(moviesDf)
  cacheTest(moviesDf)
  testingPipe(moviesDf)
  testingWhere(moviesDf)
  testingDrop(moviesDf)
  testingSqlExpr()
  testingSourceDataFrame()
  deserializerJsonTest()
  deserializerXmlTest()
  deserializerAvroTest()
  testInferSchemaJsonColumn()
  testInferSchemaDataframe(moviesDf)

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


  def testSourceTargetJdbcUserPassword(df: DataFrame): DataFrame = {
    almaren.builder
      .sourceSql(s"select * from $testTable")
      .targetJdbc("jdbc:postgresql://localhost/almaren", "org.postgresql.Driver", "movies_test", SaveMode.Overwrite, Some("postgres"), Some("foo"))
      .batch

    almaren.builder
      .sourceJdbc("jdbc:postgresql://localhost/almaren", "org.postgresql.Driver", "select * from movies_test", Some("postgres"), Some("foo"))
      .batch
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

  def testSourceFile(format:String,path:String):DataFrame ={
    almaren.builder
      .sourceFile(format,path,Map())
      .batch

  }

  def repartitionAndColaeseTest(dataFrame: DataFrame) {
    val repartition_df = almaren.builder.sourceSql(s"select * from $testTable")
      .repartition(10).batch

    repartition_df.createTempView("test_new")
    val coalase_df = almaren.builder.sourceSql("select * from test_new")
      .coalesce(5).batch

    val repartition_size = repartition_df.rdd.partitions.size
    val coalese_size = coalase_df.rdd.partitions.size

    test("repartition") {
      assert(repartition_size == 10)
    }
    test("coalesce") {
      assert(coalese_size == 5)
    }

  }

  def aliasTest(df: DataFrame): Unit = {
    almaren.builder.sourceSql(s"select * from $testTable").alias("alias_test").batch

    val aliasTableDf = spark.read.table("alias_test")
    val aliasTableCount = aliasTableDf.count()

    test("alias") {
      assert(aliasTableCount > 0)
    }
  }
  def testingDrop(moviesDf: DataFrame): Unit = {

    moviesDf.createTempView("Test_drop")

    val testDF: DataFrame = almaren.builder.sourceSql("select title,year from Test_drop").drop("title").batch
    val testDropcompare = almaren.builder.sourceSql("select year from Test_drop").batch

    test(testDF, testDropcompare, "Testing Drop")

  }
  def testingWhere(moviesDf: DataFrame): Unit = {

    moviesDf.createTempView("Test_where")

    val testDF: DataFrame = almaren.builder.sourceSql("select year from Test_where").where("year = 1990").batch
    val testWherecompare = almaren.builder.sourceSql("select year from Test_where WHERE year = '1990'").batch


    test(testDF, testWherecompare, "Testing Where")
  }
  def testingSqlExpr(): Unit = {

    val df = Seq(
      ("John", "Smith", "London", 55.3),
      ("David", "Jones", "India", 62.5),
      ("Michael", "Johnson", "Indonesia", 68.2),
      ("Chris", "Lee", "Brazil", 53.4),
      ("Mike", "Brown", "Russia", 65.6)
    ).toDF("first_name", "last_name", "country", "salary")
    df.createOrReplaceTempView("person_info")


    val testDF  = almaren.builder.sourceSql("select CAST (salary as INT) from person_info" ).batch
    val testSqlExprcompare = almaren.builder.sourceSql("select * from person_info").sqlExpr("CAST(salary as int)").batch
    test(testDF, testSqlExprcompare, "Testing sqlExpr")
 }
  def testingSourceDataFrame(): Unit = {

    val testDS = spark.range(3)
    val testCompareDf = spark.range(3).toDF
    val testDF = almaren.builder.sourceDataFrame(testDS).batch

    test(testDF, testCompareDf, "Testing SourceDF")
  }



    def cacheTest(df: DataFrame): Unit = {

    df.createTempView("cache_test")

    val testCacheDf: DataFrame = almaren.builder.sourceSql("select * from cache_test").cache(true).batch
    val bool_cache = testCacheDf.storageLevel.useMemory
    test("Testing Cache") {
      assert(bool_cache)
    }

    val testUnCacheDf = almaren.builder.sourceSql("select * from cache_test").cache(false).batch
    val bool_uncache = testUnCacheDf.storageLevel.useMemory
    test("Testing Uncache") {
      assert(!bool_uncache)
    }

  }

  def testingPipe(df: DataFrame): Unit = {
    df.createTempView("pipe_view")
    val pipeDf = almaren.builder.sql("select * from pipe_view").pipe("echo 'Testing Echo Command ' ").batch
    val pipeDfCount = pipeDf.count()
    test("Testing Pipe") {
      assert(pipeDfCount > 0)
    }
  }

  def deserializerJsonTest(): Unit = {
    val jsonStr = Seq("""{"name":"John","age":21,"address":"New York"}""",
      """{"name":"Peter","age":18,"address":"Prague"}""",
      """{"name":"Tony","age":40,"address":"New York"}""").toDF("json_string").createOrReplaceTempView("sample_json_table")

    val jsondf = almaren.builder.sourceSql("select * from sample_json_table").deserializer("JSON", "json_string").batch

    val jsonschmeadf = almaren.builder.sourceSql("select * from sample_json_table").deserializer("JSON", "json_string", Option("`address` STRING,`age` BIGINT,`name` STRING ")).batch

    val json_str = scala.io.Source.fromURL(getClass.getResource("/sample_data/person.json")).mkString
    val resDf: DataFrame = spark.read.json(Seq(json_str).toDS)

    test(jsondf, resDf, "Deserialize JSON")
    test(jsonschmeadf, resDf, "Deserialize JSON Schema")
  }

  def deserializerXmlTest(): Unit = {
    val xmlStr = Seq(
      """ <json_string>
                              <name>John</name>
                              <age>21</age>
                              <address>New York</address>
                          </json_string>""",
      """<json_string>
                              <name>Peter</name>
                              <age>18</age>
                              <address>Prague</address>
                          </json_string>""",
      """<json_string>
                              <name>Tony</name>
                              <age>40</age>
                              <address>New York</address>
                          </json_string>""").toDF("xml_string").createOrReplaceTempView("sample_xml_table")

    val xmldf = almaren.builder.sourceSql("select * from sample_xml_table").deserializer("XML", "xml_string").batch

    val xmlSchemaDf = almaren.builder.sourceSql("select * from sample_xml_table").deserializer("XML", "xml_string", Some("`address` STRING,`age` BIGINT,`name` STRING ")).batch

    val df = spark.read
      .format("xml")
      .option("rowTag", "json_string")
      .option("rootTag", "Person")
      .load("src/test/resources/sample_data/person.xml")

    test(xmldf, df, "Deserializer XML")
    test(xmlSchemaDf, df, "Deserialize XML Schema")

  }

  def deserializerAvroTest(): Unit = {
    val df = spark.range(10).select('id, 'id.cast("string").as("name"))
    val struct_df = df.select(struct('id, 'name).as("struct"))

    val avroStructDF = struct_df.select(to_avro('struct).as("avro_struct"))

    avroStructDF.createOrReplaceTempView("avro_df")
    val avroTypeStruct =
      s"""
         |{
         |  "type": "record",
         |  "name": "avro_struct",
         |  "fields": [
         |    {"name": "id", "type": "long"},
         |    {"name": "name", "type": "string"}
         |  ]
         |}
    """.stripMargin

    val avroDeserialzedDf = almaren.builder.sourceSql("select * from avro_df").deserializer("AVRO", "avro_struct", Some(avroTypeStruct)).batch

    test(df, avroDeserialzedDf, "Deserializer AVRO")
  }

  def testInferSchemaJsonColumn(): Unit = {
    val jsonStr = Seq("""{"name":"John","age":21,"address":"New York"}""",
      """{"name":"Peter","age":18,"address":"Prague"}""",
      """{"name":"Tony","age":40,"address":"New York"}""").toDF("json_string").createOrReplaceTempView("sample_json_table")

    val df = spark.sql("select * from sample_json_table")
    val jsonSchema = "`address` STRING,`age` BIGINT,`name` STRING"
    val generatedSchema = Util.genDDLFromJsonString(df, "json_string",0.1)
    testSchema(jsonSchema, generatedSchema, "Test infer schema for json column")
  }

  def testInferSchemaDataframe(df: DataFrame): Unit = {
    val dfSchema = "`cast` ARRAY<STRING>,`genres` ARRAY<STRING>,`title` STRING,`year` BIGINT"
    val generatedSchema = Util.genDDLFromDataFrame(df,0.1)
    testSchema(dfSchema, generatedSchema, "Test infer schema for dataframe")
  }

  def testSchema(jsonSchema: String, generatedSchema: String, name: String): Unit = {
    test(s"$name") {
      assert(jsonSchema.equals(generatedSchema))
    }

  }

}
