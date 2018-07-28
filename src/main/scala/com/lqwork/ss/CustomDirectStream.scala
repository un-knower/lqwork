package com.lqwork.ss

import com.typesafe.config.Config
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

object CustomDirectStream {

  def createCustomDirectKafkaStream(spark: SparkSession, config: Config, ssc: StreamingContext): InputDStream[(String, String)] ={
    //Kafka parameters
    val brokers = config.getString("application.brokers")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topics = config.getString("application.topics")
    val topicsSet = topics.split(",").toSet

    //zookeeper parameters
    val zkHosts = config.getString("application.zkHosts")
    val zkPath = config.getString("application.zkPath")

    val zKClient = new ZkClient(zkHosts, 30000, 30000)
    val storedOffsets = readOffsets(zKClient, zkHosts, zkPath)

    val messages = storedOffsets match {
      case None =>
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("lianji.lianji_order.order_rent", "lianji.lianji_order.order_sale"))
      case Some(fromOffsets) =>
        println("fromOffsets")
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    messages.foreachRDD(rdd => saveOffsets(zKClient, zkHosts, zkPath, rdd))
    messages
  }


  /*
  Read the previously saved offsets of kafka topic partitions from zookeeper
   */
  def readOffsets(zkClient: ZkClient, zkHosts:String, zkPath: String): Option[Map[TopicAndPartition, Long]] ={
    val (offsetsRangeStrOpt, _) = ZkUtils.readDataMaybeNull(zkClient, zkPath)
    offsetsRangeStrOpt match {
      case Some(offsetsRangesStr) =>
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map({
            case Array(topic, partionStr, offsetStr) => TopicAndPartition(topic, partionStr.toInt) -> offsetStr.toLong
          }).toMap
        Some(offsets)
      case None =>
        None
    }
  }


  /*
  Save offsets of each kafka partition of each kafka topic to zookeeper
   */
  def saveOffsets(zkClient: ZkClient, zkHosts:String, zkPath: String, rdd: RDD[_]): Unit ={
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetRangesStr = offsetsRanges.map(offsetsRange => s"${offsetsRange.topic}:${offsetsRange.partition}:${offsetsRange.fromOffset}")
      .mkString(",")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetRangesStr)
  }
}