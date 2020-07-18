# Almaren Framework Examples

## Read from a file and write to JDBC

```scala
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.Almaren

import org.apache.spark.sql.{DataFrame, SaveMode}

val almaren = Almaren("App Name")

val spark = almaren.spark
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "10")
    
val df:DataFrame = almaren.builder
    .sourceSql("SELECT * FROM json.`/home/files/movies.json`")
    .dsl("""id$id:LongType
        |title$title:StringType
        |year$year:LongType
        |cast[0]$actor:StringType
        |cast[1]$support_actor:StringType
        |genres[0]$genre:StringType
        |director@director
        |	director.name$credit_name:StringType""".stripMargin)
    .targetJdbc("jdbc:postgresql://localhost/almaren","org.postgresql.Driver","movies",SaveMode.Overwrite)
    .batch
```
