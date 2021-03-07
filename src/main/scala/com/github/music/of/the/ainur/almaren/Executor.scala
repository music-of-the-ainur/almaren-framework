package com.github.music.of.the.ainur.almaren

import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.almaren.util.Constants
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization._
import org.apache.spark.sql._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.annotation.Experimental

private[almaren] trait Executor extends Catalyst with Batch with Streaming

private trait Catalyst extends LazyLogging {
  // execute's PreOrder BT

  def catalyst(tree: Option[Tree],df:DataFrame = Almaren.spark.getOrCreate().emptyDataFrame): DataFrame = {
    val t = tree.getOrElse(throw NullCatalyst)
    t.c.foldLeft(t.state._executor(df))((d,container) => catalyst(container,d))
  }

  def catalyst(tree: Tree,df: DataFrame): DataFrame = {
    val nodeDf = tree.state._executor(df)
    tree.c match {
      case node :: Nil => catalyst(node,nodeDf)
      case Nil => nodeDf
      case nodes => nodesExecutor(nodes,nodeDf)
    }
  }

  private def nodesExecutor(tree: List[Tree],df: DataFrame): DataFrame = {
    tree.foldLeft(df)((d,t) => catalyst(t,df))
    df
  }
  
}

private trait Batch {
  this:Catalyst =>
  def batch(tree: Option[Tree]): DataFrame =
    catalyst(tree)
}

private trait Streaming {
this:Catalyst => 
  def streaming(tree: Option[Tree],params:Map[String,String] = Map()): Unit = {
    val spark = Almaren.spark.getOrCreate()

    import spark.implicits._

    val streamingDF = spark
      .readStream
      .format("kafka")
      .options(params)
      .load()

    val streaming = streamingDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF.createOrReplaceTempView(Constants.TempStreamTableName)
      val df = catalyst(tree,batchDF)
    }.start()

    streaming.awaitTermination()
  }

  @Experimental
  def streaming2(
    tree: Option[Tree],
    topics:String,
    bootstrapServers: String,
    groupId: String,
    autoOffsetReset: String = "earliest",
    params:Map[String,Object] = Map(),
    microBatchSecs:Long = 1,
    numStreams:Int = 5) = {
    val ssc = new StreamingContext(Almaren.spark.getOrCreate.sparkContext,Seconds(microBatchSecs))
    val kafkaParams = createKafkaStreamingParams(bootstrapServers, groupId, autoOffsetReset, params)

    val streams = (1 to numStreams).map { i => 
      KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
        ssc,
        PreferConsistent,
        Subscribe[Array[Byte], Array[Byte]](topics.split(",") , kafkaParams)
      )
    }
    val unifiedStream = ssc.union(streams)

    createKafkaStreamingJob(tree,unifiedStream)
    ssc.start()
    ssc.awaitTermination()
  }


  private def createKafkaStreamingParams(
    bootstrapServers: String,
    groupId: String, 
    autoOffsetReset: String,
    params:Map[String, Object]): Map[String,Object] =
    Map[String, Object](
      "bootstrap.servers" -> bootstrapServers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoOffsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ) ++ params

  private def createKafkaStreamingJob(
    tree: Option[Tree],
    stream: DStream[ConsumerRecord[Array[Byte],Array[Byte]]]
  ): Unit = {
    stream.foreachRDD({rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).enableHiveSupport().getOrCreate()
      import spark.implicits._

      val df = rdd.map(s => (s.key(),s.value())).toDF
      df.createOrReplaceTempView(Constants.TempStreamTableName)

      val resDf = catalyst(tree,df)
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
  }
}
