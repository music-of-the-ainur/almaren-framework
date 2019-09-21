# Almaren Framework

The Almaren Framework provides an interface layered over Apache Spark. It does all the hard work using an elegant and minimalistic syntax while still allowing you to take advantage of native Apache Spark features. You can still combine it with standard Spark code.


## Examples

### Example 1

![Example 1](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example1.png)

```scala
val almaren = Almaren("appName")
val df:DataFrame = almaren.sourceSql("SELECT * FROM db.schema.table")
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

## Example 2

![Example 2](https://raw.githubusercontent.com/music-of-the-ainur/almaren-framework/master/docs/images/example2.png)

```scala
val almaren = Almaren("appName")
val sourceData = almaren.sourceSql("SELECT * FROM db.schema.table")
    .deserializer("XML","xml_str").cache.fork
        
sourceData.dsl("uuid$id:StringType
    |code$area_code:LongType
    |names@name
    |    name.firstName$first_name:StringType
    |    name.secondName$second_name:StringType
    |    name.lastName$last_name:StringType
    |source_id$source_id:LongType".stripMargin)
.sql("SELECT *,unix_timestamp() as timestamp from __TABLE__")
.targetSql("INSERT INTO TABLE output1 SELECT * FROM __TABLE__")
    
sourceData.dsl("uuid$id:StringType
    |code$area_code:LongType
    |phones@phone
    |    phone.number$phone_number:StringType
    |source_id$source_id:LongType".stripMargin)
.sql("SELECT *,unix_timestamp() as timestamp from __TABLE__")
.targetSql("INSERT INTO TABLE output2 SELECT * FROM __TABLE__")

sourceData.batch
```
