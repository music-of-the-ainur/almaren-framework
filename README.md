# Almaren Framework

The Almaren Framework provides a simplified consistent minimalistic layer over Apache Spark, while still allowing you to take advantage of native Apache Spark features. You can even combine it with standard Spark code.

[![Build Status](https://github.com/music-of-the-ainur/almaren-framework/actions/workflows/almaren-framework.yml/badge.svg)](https://github.com/music-of-the-ainur/almaren-framework/actions/workflows/almaren-framework.yml)
[![Gitter Community](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/music-of-the-ainur/community)

## Table of Contents

- [Introduction](#introduction)
  * [Dependency](#dependency)
  * [Batch Example](#batch-example)
  * [Streaming Example](#streaming-example)
- [Debugging](#debugging)
- [Components](#components)
  * [Source](#source)
    + [sourceDataFrame](#sourceDataFrame)
    + [sourceSql](#sourcesql)
    + [sourceFile](#sourcefile)
    + [sourceJdbc](#sourcejdbc)
    + [sourceSolr](#sourcesolr)
    + [sourceMongoDb](#sourcemongodb)
    + [sourceBigQuery](#sourcebigquery)
  * [Main](#main)
    + [Cache](#cache)
    + [Coalesce](#coalesce)
    + [Repartition](#repartition)
    + [Pipe](#pipe)
    + [Alias](#alias)
    + [Deserializer](#deserializer)
    + [SQL](#sql)
    + [DSL](#dsl)
    + [HTTP](#http)
    + [SqlExpr](#sqlExpr)
    + [Where](#where)
    + [Drop](#drop)
  * [Target](#target)
    + [targetSql](#targetsql)
    + [targetJdbc](#targetjdbc)
    + [targetKafka](#targetkafka)
    + [targetSolr](#targetsolr)
    + [targetMongoDb](#targetmongodb)
    + [targetBigQuery](#targetbigquery)
    + [targetFile](#targetfile)
- [Executors](#executors)
  * [Batch](#batch)
  * [Streaming Kafka](#streaming-kafka)
- [Examples](#examples)
  * [Example 1](#example-1)
  * [Example 2](#example-2)
  * [Example 3](#example-3)
  * [Example 4](#example-4)

## Introduction

The Almaren Framework provides a simplified consistent minimalistic layer over Apache Spark, while still allowing you to take advantage of native Apache Spark features. You can even combine it with standard Spark code.

### Dependency

To add Almaren Framework dependency to your sbt build:

```
libraryDependencies += "com.github.music-of-the-ainur" %% "almaren-framework" % "0.9.12-3.2"
```

To run in spark-shell:

```
spark-shell --packages "com.github.music-of-the-ainur:almaren-framework_2.12:0.9.12-3.2"
```


Almaren connector is available in 
[Maven Central](https://mvnrepository.com/artifact/com.github.music-of-the-ainur)
repository. 

| version                    | Connector Artifact                                                |
|----------------------------|-------------------------------------------------------------------|
| Spark 3.5.x and scala 2.13 | `com.github.music-of-the-ainur:almaren-framework_2.13:0.9.12-3.5` |
| Spark 3.5.x and scala 2.12 | `com.github.music-of-the-ainur:almaren-framework_2.12:0.9.12-3.5` |
| Spark 3.4.x and scala 2.13 | `com.github.music-of-the-ainur:almaren-framework_2.13:0.9.12-3.4` |
| Spark 3.4.x and scala 2.12 | `com.github.music-of-the-ainur:almaren-framework_2.12:0.9.12-3.4` |
| Spark 3.3.x and scala 2.13 | `com.github.music-of-the-ainur:almaren-framework_2.13:0.9.12-3.3` |
| Spark 3.3.x and scala 2.12 | `com.github.music-of-the-ainur:almaren-framework_2.12:0.9.12-3.3` |
| Spark 3.2.x and scala 2.12 | `com.github.music-of-the-ainur:almaren-framework_2.12:0.9.12-3.2` |
| Spark 3.1.x and scala 2.12 | `com.github.music-of-the-ainur:almaren-framework_2.12:0.9.12-3.1` |
| Spark 2.4.x and scala 2.12 | `com.github.music-of-the-ainur:almaren-framework_2.12:0.9.11-2.4` |
| Spark 2.4.x and scala 2.11 | `com.github.music-of-the-ainur:almaren-framework_2.11:0.9.11-2.4` |

### Batch Example
```scala
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.Almaren

import org.apache.spark.sql.DataFrame

val almaren = Almaren("App Name")

val spark = almaren.spark
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    
val df:DataFrame = almaren.builder
    .sourceSql("select monotonically_increasing_id() as id,* from movies")
    .dsl("""id$id:LongType
        |title$title:StringType
        |year$year:LongType
        |cast[0]$actor:StringType
        |cast[1]$support_actor:StringType
        |genres[0]$genre:StringType
        |director@director
        |	director.name$credit_name:StringType""".stripMargin).alias("table")
    .sql("""SELECT * FROM table WHERE actor NOT IN ("the","the life of")""")
    .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)
    .batch
```


### Streaming Example

```scala
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.Almaren

val almaren = Almaren("Streaming App")

val streaming = almaren.builder
    .sourceSql("select CAST(value AS STRING) as json_column FROM __STREAMING__")
    .deserializer("json","json_column")
    .dsl("""user.id$user_id:LongType
        |user.name$user_name:StringType
        |user.time_zone$time_zone:StringType
        |user.friends_count$friends_count:LongType
        |user.followers_count$followers_count:LongType
        |source$source:StringType
        |place.country$country:StringType
        |timestamp_ms$timestamp_ms:LongType
        |text$message:StringType
        |entities@entitie
        |	entitie.hashtags@hashtag
        |		hashtag.text$hashtag:StringType""".stripMargin).alias("table")
  .sql("SELECT DISTINCT * FROM table").alias("table1")
  .sql("""SELECT sha2(concat_ws("",array(*)),256) as unique_hash,*,current_timestamp from table1""")
  .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","twitter_streaming",SaveMode.Append)

almaren.streaming(streaming,Map("kafka.bootstrap.servers" -> "localhost:9092","subscribe" -> "twitter", "startingOffsets" -> "earliest"))

```
## Debugging

To debug the code you can turn on ```log4j.logger.com.github.music.of.the.ainur.almaren=DEBUG```, so you can see the state of each component.

You also can setup the debug from Scala code:

```scala
import org.apache.log4j.{Level, Logger}
Logger.getLogger("com.github.music.of.the.ainur.almaren").setLevel(Level.DEBUG)
```

Example:

```scala
val df:DataFrame = almaren.builder
    .sourceSql("select monotonically_increasing_id() as id,* from movies")
    .dsl("""id$id:LongType
        |title$title:StringType
        |year$year:LongType
        |cast[0]$actor:StringType
        |cast[1]$support_actor:StringType
        |genres[0]$genre:StringType
        |director@director
        |	director.name$credit_name:StringType""".stripMargin).alias("table")
    .sql("""SELECT *,current_timestamp as date FROM table WHERE actor NOT IN ("the","the life of")""")
    .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)
    .batch
```

The output:

```
20/10/08 11:57:10 INFO SourceSql: sql:{select monotonically_increasing_id() as id,* from movies}
+---+----------------+--------------------+--------------------+----+
| id|            cast|              genres|               title|year|
+---+----------------+--------------------+--------------------+----+
|  0|              []|                  []|After Dark in Cen...|1900|
|  1|              []|                  []|Boarding School G...|1900|
|  2|              []|                  []|Buffalo Bill's Wi...|1900|
|  3|              []|                  []|              Caught|1900|
|  4|              []|                  []|Clowns Spinning Hats|1900|
|  5|              []|[Short, Documentary]|Capture of Boer B...|1900|
|  6|              []|                  []|The Enchanted Dra...|1900|
|  7|   [Paul Boyton]|                  []|   Feeding Sea Lions|1900|
|  8|              []|            [Comedy]|How to Make a Fat...|1900|
|  9|              []|                  []|     New Life Rescue|1900|
| 10|              []|                  []|    New Morning Bath|1900|
| 11|              []|                  []|Searching Ruins o...|1900|
| 12|              []|                  []|The Tribulations ...|1900|
| 13|              []|            [Comedy]|Trouble in Hogan'...|1900|
| 14|              []|             [Short]|      Two Old Sparks|1900|
| 15|[Ching Ling Foo]|             [Short]|The Wonder, Ching...|1900|
| 16|              []|             [Short]|  Watermelon Contest|1900|
| 17|              []|                  []|   Acrobats in Cairo|1901|
| 18|              []|                  []|  An Affair of Honor|1901|
| 19|              []|                  []|Another Job for t...|1901|
+---+----------------+--------------------+--------------------+----+
only showing top 20 rows

20/10/08 11:57:10 INFO Dsl: dsl:{id$id:LongType
title$title:StringType
year$year:LongType
cast[0]$actor:StringType
cast[1]$support_actor:StringType
genres[0]$genre:StringType
director@director
	director.name$credit_name:StringType}
+---+--------------------+----+--------------+-------------+------+-----------+
| id|               title|year|         actor|support_actor| genre|credit_name|
+---+--------------------+----+--------------+-------------+------+-----------+
|  0|After Dark in Cen...|1900|          null|         null|  null|       null|
|  1|Boarding School G...|1900|          null|         null|  null|       null|
|  2|Buffalo Bill's Wi...|1900|          null|         null|  null|       null|
|  3|              Caught|1900|          null|         null|  null|       null|
|  4|Clowns Spinning Hats|1900|          null|         null|  null|       null|
|  5|Capture of Boer B...|1900|          null|         null| Short|       null|
|  6|The Enchanted Dra...|1900|          null|         null|  null|       null|
|  7|   Feeding Sea Lions|1900|   Paul Boyton|         null|  null|       null|
|  8|How to Make a Fat...|1900|          null|         null|Comedy|       null|
|  9|     New Life Rescue|1900|          null|         null|  null|       null|
| 10|    New Morning Bath|1900|          null|         null|  null|       null|
| 11|Searching Ruins o...|1900|          null|         null|  null|       null|
| 12|The Tribulations ...|1900|          null|         null|  null|       null|
| 13|Trouble in Hogan'...|1900|          null|         null|Comedy|       null|
| 14|      Two Old Sparks|1900|          null|         null| Short|       null|
| 15|The Wonder, Ching...|1900|Ching Ling Foo|         null| Short|       null|
| 16|  Watermelon Contest|1900|          null|         null| Short|       null|
| 17|   Acrobats in Cairo|1901|          null|         null|  null|       null|
| 18|  An Affair of Honor|1901|          null|         null|  null|       null|
| 19|Another Job for t...|1901|          null|         null|  null|       null|
+---+--------------------+----+--------------+-------------+------+-----------+
only showing top 20 rows

20/10/08 11:57:11 INFO Sql: sql:{SELECT *,current_timestamp as date FROM __TABLE__ WHERE actor NOT IN ("the","the life of")}
+---+--------------------+----+------------------+-----------------+----------+-----------+--------------------+
| id|               title|year|             actor|    support_actor|     genre|credit_name|                date|
+---+--------------------+----+------------------+-----------------+----------+-----------+--------------------+
|  7|   Feeding Sea Lions|1900|       Paul Boyton|             null|      null|       null|2020-10-08 11:57:...|
| 15|The Wonder, Ching...|1900|    Ching Ling Foo|             null|     Short|       null|2020-10-08 11:57:...|
|105| Alice in Wonderland|1903|         May Clark|             null|      null|       null|2020-10-08 11:57:...|
|142|   Nicholas Nickleby|1903|William Carrington|             null|      null|       null|2020-10-08 11:57:...|
|242|The Automobile Th...|1906|J. Stuart Blackton|Florence Lawrence|     Short|       null|2020-10-08 11:57:...|
|245|Humorous Phases o...|1906|J. Stuart Blackton|             null|     Short|       null|2020-10-08 11:57:...|
|250|             Ben-Hur|1907|   William S. Hart|             null|Historical|       null|2020-10-08 11:57:...|
|251|        Daniel Boone|1907|    William Craven|Florence Lawrence| Biography|       null|2020-10-08 11:57:...|
|252|How Brown Saw the...|1907|           Unknown|             null|    Comedy|       null|2020-10-08 11:57:...|
|253|        Laughing Gas|1907|   Bertha Regustus|   Edward Boulden|    Comedy|       null|2020-10-08 11:57:...|
|256|The Adventures of...|1908| Arthur V. Johnson|   Linda Arvidson|     Drama|       null|2020-10-08 11:57:...|
|257|Antony and Cleopatra|1908| Florence Lawrence|William V. Ranous|      null|       null|2020-10-08 11:57:...|
|258| Balked at the Altar|1908|    Linda Arvidson|  George Gebhardt|    Comedy|       null|2020-10-08 11:57:...|
|259|The Bandit's Wate...|1908|    Charles Inslee|   Linda Arvidson|     Drama|       null|2020-10-08 11:57:...|
|260|     The Black Viper|1908|    D. W. Griffith|             null|     Drama|       null|2020-10-08 11:57:...|
|261|A Calamitous Elop...|1908|      Harry Solter|   Linda Arvidson|    Comedy|       null|2020-10-08 11:57:...|
|262|The Call of the Wild|1908|    Charles Inslee|             null| Adventure|       null|2020-10-08 11:57:...|
|263|   A Christmas Carol|1908|      Tom Ricketts|             null|     Drama|       null|2020-10-08 11:57:...|
|264|Deceived Slumming...|1908|     Edward Dillon|   D. W. Griffith|    Comedy|       null|2020-10-08 11:57:...|
|265|Dr. Jekyll and Mr...|1908|   Hobart Bosworth|      Betty Harte|    Horror|       null|2020-10-08 11:57:...|
+---+--------------------+----+------------------+-----------------+----------+-----------+--------------------+
only showing top 20 rows

20/10/08 11:57:11 INFO TargetJdbc: url:{jdbc:postgresql://localhost/almaren}, driver:{org.postgresql.Driver}, dbtable:{movies}, user:{None}, params:{Map()}
+---+--------------------+----+------------------+-----------------+----------+-----------+--------------------+
| id|               title|year|             actor|    support_actor|     genre|credit_name|                date|
+---+--------------------+----+------------------+-----------------+----------+-----------+--------------------+
|  7|   Feeding Sea Lions|1900|       Paul Boyton|             null|      null|       null|2020-10-08 11:57:...|
| 15|The Wonder, Ching...|1900|    Ching Ling Foo|             null|     Short|       null|2020-10-08 11:57:...|
|105| Alice in Wonderland|1903|         May Clark|             null|      null|       null|2020-10-08 11:57:...|
|142|   Nicholas Nickleby|1903|William Carrington|             null|      null|       null|2020-10-08 11:57:...|
|242|The Automobile Th...|1906|J. Stuart Blackton|Florence Lawrence|     Short|       null|2020-10-08 11:57:...|
|245|Humorous Phases o...|1906|J. Stuart Blackton|             null|     Short|       null|2020-10-08 11:57:...|
|250|             Ben-Hur|1907|   William S. Hart|             null|Historical|       null|2020-10-08 11:57:...|
|251|        Daniel Boone|1907|    William Craven|Florence Lawrence| Biography|       null|2020-10-08 11:57:...|
|252|How Brown Saw the...|1907|           Unknown|             null|    Comedy|       null|2020-10-08 11:57:...|
|253|        Laughing Gas|1907|   Bertha Regustus|   Edward Boulden|    Comedy|       null|2020-10-08 11:57:...|
|256|The Adventures of...|1908| Arthur V. Johnson|   Linda Arvidson|     Drama|       null|2020-10-08 11:57:...|
|257|Antony and Cleopatra|1908| Florence Lawrence|William V. Ranous|      null|       null|2020-10-08 11:57:...|
|258| Balked at the Altar|1908|    Linda Arvidson|  George Gebhardt|    Comedy|       null|2020-10-08 11:57:...|
|259|The Bandit's Wate...|1908|    Charles Inslee|   Linda Arvidson|     Drama|       null|2020-10-08 11:57:...|
|260|     The Black Viper|1908|    D. W. Griffith|             null|     Drama|       null|2020-10-08 11:57:...|
|261|A Calamitous Elop...|1908|      Harry Solter|   Linda Arvidson|    Comedy|       null|2020-10-08 11:57:...|
|262|The Call of the Wild|1908|    Charles Inslee|             null| Adventure|       null|2020-10-08 11:57:...|
|263|   A Christmas Carol|1908|      Tom Ricketts|             null|     Drama|       null|2020-10-08 11:57:...|
|264|Deceived Slumming...|1908|     Edward Dillon|   D. W. Griffith|    Comedy|       null|2020-10-08 11:57:...|
|265|Dr. Jekyll and Mr...|1908|   Hobart Bosworth|      Betty Harte|    Horror|       null|2020-10-08 11:57:...|
+---+--------------------+----+------------------+-----------------+----------+-----------+--------------------+
only showing top 20 rows
```

## Components

### Source
#### sourceDataFrame

Read from an existing DataFrame

```scala
sourceDataFrame(df)
```

#### sourceSql

Read native Spark/Hive tables using Spark SQL.

```scala
sourceSql("select monotonically_increasing_id() as id,* from database.tabname")
```

#### sourceFile

Read files like CSV,Avro,JSON and XML

```scala
sourceFile("csv","/tmp/file.csv",Map("header" -> "true"))
```

#### sourceJdbc

Read from JDBC using [Spark JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

```scala
sourceJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","select * from table_name",Some("user"),Some("password"))
```

#### sourceSolr

Read from Solr using [Solr Connector](https://github.com/music-of-the-ainur/solr.almaren)

#### sourceMongoDb

Read from MongoDB using [MongoDB Connector](https://github.com/music-of-the-ainur/mongodb.almaren)

#### sourceBigQuery

Read from BigQuery using [BigQuery Connector](https://github.com/music-of-the-ainur/bigquery.almaren)

#### sourceNeo4j

Read from Neo4j using [Neo4j Connector](https://github.com/music-of-the-ainur/neo4j.almaren)

### Main

#### Cache

Cache/Uncache both DataFrame or Table

```scala
cache(true)
```

Cache Dataframe with Storage Level

```scala
cache(true,storageLevel = MEMORY_AND_DISK)
```

#### Coalesce

Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.

```scala
coalesce(10)
```

#### Repartition

Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network.

##### Repartition using Size 
```scala
repartition(100)
```

##### Repartition using Columns 
```scala
repartition(col("name")) 
```

##### Repartition using Size and Columns

```scala
repartition(100,col("name")) 
```

#### Pipe

Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.

```scala
pipe("""perl -npE 's/(?:\d+)\s+([^\w]+)/:$1/mg'""")
```

#### Alias

Creates a temporary view using the previews component, `createOrReplaceTempView`.

```scala
alias("my_table")

```

#### Deserializer

Deserialize the following types XML, JSON and Avro to Spark DataFrame.

```scala
deserializer("JSON","column_name","`cast` ARRAY<STRING>,`genres` ARRAY<STRING>,`title` STRING,`year` BIGINT")

```

#### SqlExpr 

Selects a set of SQL expressions, like `selectExpr`.

```scala
sqlExpr("*","foo as baz","a as b")
```

#### Where

Filters rows using the given SQL expression.

```scala
where("age > 15")
```

#### Drop

Returns a new Dataset with columns dropped.

```scala
drop("col_1","col_2")
```

#### SQL

[Spark SQL](https://docs.databricks.com/spark/latest/spark-sql/index.html) syntax. You can query preview component through the special table `__TABLE__`.

```scala
sql("SELECT * FROM __TABLE__")
```
#### DSL

DSL(Domain Specific Language) simplifies the task to flatten, select, alias and properly set the datatype. It's very powerful to parse complex data structures.

```scala
dsl("""title$title:StringType
	|year$year:LongType
	|cast[0]$actor:StringType
	|cast[1]$support_actor:StringType
	|genres[0]$genre:StringType""".stripMargin)
```

#### HTTP

[HTTP Connector](https://github.com/music-of-the-ainur/http.almaren) to perform HTTP requests.

### Target

#### targetSql

Write native Spark/Hive tables using [Spark SQL](https://docs.databricks.com/spark/latest/spark-sql/language-manual/insert.html).

```scala
targetSql("INSERT OVERWRITE TABLE database.table SELECT * FROM __TABLE__")
```

#### targetJdbc

Write to JDBC using [Spark JDBC](https://spark.apache.org/docs/latest/sql-data-targets-jdbc.html)

```scala
targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)
```

#### targetKafka

Write to Kafka, you must have a column named **value**, the content of this column will be sent to Kafka. You can specify the *topic* either with a column named **topic** or in the option as in the example below.
Check the [documentation](https://spark.apache.org/docs/2.4.0/structured-streaming-kafka-integration.html) for the full list of parameters

```scala
sql("SELECT to_json(struct(*)) as value FROM __TABLE__").targetKafka("localhost:9092",Map("topic" -> "testing"))

```

#### targetSolr

Write to Solr using [Solr Connector](https://github.com/music-of-the-ainur/solr.almaren)

#### targetMongoDb

Write to MongoDB using [MongoDB Connector](https://github.com/music-of-the-ainur/mongodb.almaren)

#### targetBigQuery

Write to BigQuery using [BigQuery Connector](https://github.com/music-of-the-ainur/bigquery.almaren)

#### targetNeo4j

Write to Neo4j using [Neo4j Connector](https://github.com/music-of-the-ainur/neo4j.almaren)

#### targetFile

Write to File, you must have the following parameters: format, path, saveMode of the file and parameters as a Map. For partitioning provide a list of columns, for bucketing provide number of buckets and list of columns, for sorting provide list of columns, and tableName. Check the [documentation](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html) for the full list of parameters.

```scala
targetFile("parquet","/home/abc/targetlocation/output.parquet",SaveMode.Overwrite,Map("batchSize"->10000),List("partitionColumns"),(5,List("bucketingColumns")),List("sortingColumns"),Some("sampleTableName"))
```

## Executors

Executors are responsible to execute Almaren Tree i.e ```Option[Tree]``` to Apache Spark. Without invoke an _executor_, code won't be executed by Apache Spark. Follow the list of _executors_:

### Batch

Executes the Almaren Tree returning a Dataframe.

```scala
val tree = almaren.builder
    .sourceSql("select monotonically_increasing_id() as id,* from movies")
    .dsl("""id$id:LongType
        |title$title:StringType
        |year$year:LongType
        |cast[0]$actor:StringType
        |cast[1]$support_actor:StringType
        |genres[0]$genre:StringType
        |director@director
        |	director.name$credit_name:StringType""".stripMargin).alias("table")
    .sql("""SELECT * FROM table WHERE actor NOT IN ("the","the life of")""")
    .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)

val df:DataFrame = tree.batch
```

### Streaming Kafka

Read data from Kafka and execute's Almaren Tree providing the special table ```__STREAMING__```:

| Column Name   | Data Type |
|---------------|-----------|
| key           | binary    |
| value         | binary    |
| topic         | string    |
| partition     | int       |
| offset        | long      |
| timestamp     | long      |
| timestampType | int       |

The ```streaming(tree,params:Map[String,String]``` method _params_ are the options available in ```readStream.format("kafka").options(params)``` you can check all the options [here](https://spark.apache.org/docs/2.4.0/structured-streaming-kafka-integration.html#kafka-specific-configurations)

```scala
val tree = almaren.builder
    .sourceSql("select CAST(value AS STRING) as json_column FROM __STREAMING__")
    .deserializer("json","json_column")
    .dsl("""user.id$user_id:LongType
        |user.name$user_name:StringType
        |user.time_zone$time_zone:StringType
        |user.friends_count$friends_count:LongType
        |user.followers_count$followers_count:LongType
        |source$source:StringType
        |place.country$country:StringType
        |timestamp_ms$timestamp_ms:LongType
        |text$message:StringType
        |entities@entitie
        |	entitie.hashtags@hashtag
        |		hashtag.text$hashtag:StringType""".stripMargin).alias("table")
  .sql("SELECT DISTINCT * FROM table").alias("table1")
  .sql("""SELECT sha2(concat_ws("",array(*)),256) as unique_hash,*,current_timestamp from table1""")
  .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","twitter_streaming",SaveMode.Append)

almaren.streaming(tree,Map("kafka.bootstrap.servers" -> "localhost:9092","subscribe" -> "twitter", "startingOffsets" -> "earliest"))
```

## Util 

### Generate Schema 

To generate DDL for a json string column of a Dataframe, provide dataframe, JSON string column name and the sample ratio.


```scala
import com.github.music.of.the.ainur.almaren.Util

val schema = Util.genDDLFromJsonString(df, "person_info",0.1)
```

To generate DDL for a Dataframe , provide dataframe and the sample ratio

```scala
import com.github.music.of.the.ainur.almaren.Util

val schema = Util.genDDLFromDataFrame(df,0.1)
```
Default value of sample ratio is 1.0


## Examples

### Example 1

![Example 1](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example1.png)

```scala
val almaren = Almaren("appName")
val df:DataFrame = almaren.builder.sourceSql("SELECT * FROM db.schema.table")
    .deserializer("JSON","json_str")
    .dsl("""uuid$id:StringType
        |code$area_code:LongType
        |names@name
        |	name.firstName$first_name:StringType
        |	name.secondName$second_name:StringType
        |	name.lastName$last_name:StringType
        |source_id$source_id:LongType""".stripMargin).alias("table")
    .sql("""SELECT *,unix_timestamp() as timestamp from table""")
    .targetJdbc("jdbc:postgresql://localhost/database","org.postgresql.Driver","target_table",SaveMode.Append)
```

### Example 2

![Example 2](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example2.png)

```scala
val almaren = Almaren("appName")
        
val target1 = almaren.builder.dsl("""uuid$id:StringType
    |code$area_code:LongType
    |names@name
    |    name.firstName$first_name:StringType
    |    name.secondName$second_name:StringType
    |    name.lastName$last_name:StringType
    |source_id$source_id:LongType""".stripMargin).alias("table")
.sql("SELECT *,unix_timestamp() as timestamp from table")
.targetCassandra("test1","kv1")
    
val target2 = almaren.builder.dsl("""uuid$id:StringType
    |code$area_code:LongType
    |phones@phone
    |    phone.number$phone_number:StringType
    |source_id$source_id:LongType""".stripMargin).alias("table")
.sql("SELECT *,unix_timestamp() as timestamp from table")
.targetCassandra("test2","kv2")

almaren.builder.sourceSql("SELECT * FROM db.schema.table")
    .deserializer("XML","xml_str").cache.fork(target1,target2)
    .batch
```

### Example 3

![Example 3](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example3.png)

```scala
val almaren = Almaren("appName")

val sourcePolicy = almaren.builder.sourceHbase("""{
    |"table":{"namespace":"default", "name":"policy"},
    |"rowkey":"id",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"id", "type":"long"},
    |"number":{"cf":"Policy", "col":"number", "type":"long"},
    |"source":{"cf":"Policy", "col":"source", "type":"string"},
    |"status":{"cf":"Policy", "col":"status", "type":"string"},
    |"person_id":{"cf":"Policy", "col":"source", "type":"long"}
    |}
|}""").alias("hbase")
        .sql(""" SELECT * FROM hbase WHERE status = "ACTIVE" """).alias("policy")

val sourcePerson = almaren.builder.sourceHbase("""{
    |"table":{"namespace":"default", "name":"person"},
    |"rowkey":"id",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"id", "type":"long"},
    |"name":{"cf":"Policy", "col":"number", "type":"string"},
    |"type":{"cf":"Policy", "col":"type", "type":"string"},
    |"age":{"cf":"Policy", "col":"source", "type":"string"}
    |}
    |}""").alias("hbase")
        .sql(""" SELECT * FROM hbase WHERE type = "PREMIUM" """).alias("person")

almaren.builder.sql(""" SELECT * FROM person JOIN policy ON policy.person_id = person.id """).alias("table")
    .sql("SELECT *,unix_timestamp() as timestamp FROM table").alias("table1")
    .coalesce(100)
    .targetSql("INSERT INTO TABLE area.premimum_users SELECT * FROM table1")
    .batch(sourcePolicy,sourceHbase)
```

### Example 4

![Example 4](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example4.png)

```scala
val almaren = Almaren("appName")
val sourceData = almaren.builder.sourceJdbc("oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:@localhost:1521:xe","SELECT * FROM schema.table WHERE st_date >= (sysdate-1) AND st_date < sysdate").alias("table")
    .sql("SELECT to_json(named_struct('id', id,))) as __BODY__ from table")
    .coalesce(30)
    .targetHttp("https://host.com:9093/api/foo","post",Map("Authorization" -> "Basic QWxhZGRpbjpPcGVuU2VzYW1l"))
    
sourceData.batch
```

## Author

Daniel Mantovani [daniel.mantovani@modak.com](mailto:daniel.mantovani@modak.com)

## Sponsor
[![Modak Analytics](/docs/images/modak_analytics.png)](http://www.modak.com)
