# Almaren Framework

[![Build Status](https://travis-ci.org/music-of-the-ainur/almaren-framework.svg?branch=master)](https://travis-ci.org/music-of-the-ainur/almaren-framework)

The Almaren Framework provides a simplified consistent minimalistic layer over Apache Spark. While still allowing you to take advantage of native Apache Spark features. You can still combine it with standard Spark code.

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import org.apache.spark.sql.DataFrame

val almaren = Almaren("App Name")

val spark = almaren.spark
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "1")
    
val movies = almaren.builder
    .sourceSql("select monotonically_increasing_id() as id,* from movies")
    .dsl("""id$id:LongType
        |title$title:StringType
        |year$year:LongType
        |cast[0]$actor:StringType
        |cast[1]$support_actor:StringType
        |genres[0]$genre:StringType
        |director@director
        |	director.name$credit_name:StringType""".stripMargin)
    .sql("""SELECT * FROM __TABLE__ WHERE actor NOT IN ("the","the life of")""")
    .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)
    
val df:DataFrame = almaren.batch(movies)
```

To add Almaren Framework dependency to your sbt build:

```
libraryDependencies += "com.github.music-of-the-ainur" %% "almaren-framework" % "0.0.1-2-3"
```

## Components

### sourceSql

Read native Spark/Hive tables using Spark SQL.

```scala
sourceSql("select monotonically_increasing_id() as id,* from database.tabname")
```

### sourceHbase

Read from Hbase using [HBase Connector](https://github.com/hortonworks-spark/shc)

### sourceCassandra

Read from Cassandra using [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector)

### sourceJdbc

Read from JDBC using [Spark JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

```scala
sourceJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","select * from database.tabname")
```

### sourceBigQuery

Read from BigQuery using [Google BigQuery Connector](https://github.com/GoogleCloudPlatform/spark-bigquery-connector)

### Cache

Cache/Uncache both DataFrame or Table

```scala
cache(true)
```

### Coalesce

Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset.

```scala
coalesce(10)
```

### Repartition

Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network.

```scala
repartition(100)
```

### Pipe

Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings.

```scala
pipe("""perl -npE 's/(?:\d+)\s+([^\w]+)/:$1/mg'""")
```

### Alias

Creates a temporary view using the previews component, `createOrReplaceTempView`.

```scala
alias("my_table")

```

### Deserializer

Deserialize the following types XML and JSON to Spark DataFrame.

```scala
deserializer("JSON","column_name","`cast` ARRAY<STRING>,`genres` ARRAY<STRING>,`title` STRING,`year` BIGINT")

```

### SQL

[Spark SQL](https://docs.databricks.com/spark/latest/spark-sql/index.html) syntax. You can query preview component through the special table `__TABLE__`.

```scala
sql("SELECT * FROM __TABLE__")
```
### DSL

DSL(Domain Specific Language) simplifies the task to flatten, select, alias and properly set the datatype. It's very powerful to parser complex data structures.

```scala
dsl("""title$title:StringType
	|year$year:LongType
	|cast[0]$actor:StringType
	|cast[1]$support_actor:StringType
	|genres[0]$genre:StringType""".stripMargin)
```

### HTTP

Start a HTTP keep-alive connection for each partition of the RDD and send a request for each row returning two columns, `header` and `body`.

### targetSql

Write native Spark/Hive tables using [Spark SQL](https://docs.databricks.com/spark/latest/spark-sql/language-manual/insert.html).

```scala
targetSql("INSERT OVERWRITE TABLE database.table SELECT * FROM __TABLE__")
```

### targetHbase

Write to Hbase using [HBase Connector](https://github.com/hortonworks-spark/shc)

### targetCassandra

Write to Cassandra using [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector)

### targetJdbc

Write to JDBC using [Spark JDBC](https://spark.apache.org/docs/latest/sql-data-targets-jdbc.html)

```scala
targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)
```

### targetHttp

Start a HTTP keep-alive connection for each partition of the RDD and send a request for each row.

### targetBigQuery

Read from BigQuery using [Google BigQuery Connector](https://github.com/GoogleCloudPlatform/spark-bigquery-connector)

## Examples

### Example 1

![Example 1](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example1.png)

```scala
val almaren = Almaren("appName")
val df:DataFrame = almaren.builder.sourceSql("SELECT * FROM db.schema.table")
    .deserializer("JSON","json_str")
    .dsl("uuid$id:StringType
        |code$area_code:LongType
        |names@name
        |	name.firstName$first_name:StringType
        |	name.secondName$second_name:StringType
        |	name.lastName$last_name:StringType
        |source_id$source_id:LongType".stripMargin)
    .sql("""SELECT *,unix_timestamp() as timestamp from __TABLE__""")
    .targetSql("INSERT OVERWRITE TABLE default.target_table SELECT * FROM __TABLE__")
    .batch
```

### Example 2

![Example 2](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example2.png)

```scala
val almaren = Almaren("appName")
        
val target1 = almaren.builder.dsl("uuid$id:StringType
    |code$area_code:LongType
    |names@name
    |    name.firstName$first_name:StringType
    |    name.secondName$second_name:StringType
    |    name.lastName$last_name:StringType
    |source_id$source_id:LongType".stripMargin)
.sql("SELECT *,unix_timestamp() as timestamp from __TABLE__")
.targetCassandra("test1","kv1")
    
val target2 = almaren.builder.dsl("uuid$id:StringType
    |code$area_code:LongType
    |phones@phone
    |    phone.number$phone_number:StringType
    |source_id$source_id:LongType".stripMargin)
.sql("SELECT *,unix_timestamp() as timestamp from __TABLE__")
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
|}""").sql(""" SELECT * FROM __TABLE__ WHERE status = "ACTIVE" """).alias("policy")

val sourcePerson = almaren.builder.sourceHbase("""{
    |"table":{"namespace":"default", "name":"person"},
    |"rowkey":"id",
    |"columns":{
    |"rowkey":{"cf":"rowkey", "col":"id", "type":"long"},
    |"name":{"cf":"Policy", "col":"number", "type":"string"},
    |"type":{"cf":"Policy", "col":"type", "type":"string"},
    |"age":{"cf":"Policy", "col":"source", "type":"string"}
    |}
|}""").sql(""" SELECT * FROM __TABLE__ WHERE type = "PREMIUM" """).alias("person")

almaren.builder.sql(""" SELECT * FROM person JOIN policy ON policy.person_id = person.id """)
    .sql("SELECT *,unix_timestamp() as timestamp FROM __TABLE__")
    .coalesce(100)
    .targetSql("INSERT INTO TABLE area.premimum_users SELECT * FROM __TABLE__")
    .batch(sourcePolicy,sourceHbase)
```

### Example 4

![Example 4](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example4.png)

```scala
val almaren = Almaren("appName")
val sourceData = almaren.builder.sourceJdbc("oracle.jdbc.driver.OracleDriver","jdbc:oracle:thin:@localhost:1521:xe","SELECT * FROM schema.table WHERE st_date >= (sysdate-1) AND st_date < sysdate")
    .sql("SELECT to_json(named_struct('id', id,))) as __BODY__ from __TABLE__")
    .coalesce(30)
    .targetHttp("https://host.com:9093/api/foo","post",Map("Authorization" -> "Basic QWxhZGRpbjpPcGVuU2VzYW1l"))
    
sourceData.batch
```

## Author

Daniel Mantovani [daniel.mantovani@modakanalytics.com](mailto:daniel.mantovani@modakanalytics.com)

## Sponsor
[![Modak Analytics](/docs/img/modak_analytics.png)](http://www.modakanalytics.com)
