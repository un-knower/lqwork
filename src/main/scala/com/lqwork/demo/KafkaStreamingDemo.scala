package com.lqwork.demo

import com.typesafe.config.Config
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json.JSONObject

object KafkaStreamingDemo {
  def kafkaSaveToHive(spark: SparkSession, config: Config, ssc: StreamingContext): Unit ={
    //Kafka parameters
    val brokers = config.getString("application.brokers")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topics = config.getString("application.topics")
    val topicsSet = topics.split(",").toSet

    //start from the latest offsets
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("jybk.jybk.jybk_orders"))

    //Processing json and save into Hive table.
    import spark.implicits._
    messages.map(_._2).foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        println("no kafka data")
      } else {
        val rdd_json = rdd.map(m => {
          val json = new JSONObject(m)
          val ts_ms = json.get("ts_ms").toString
          val after = json.getJSONObject("after")
          after.toString()
        })

        spark.read.json(rdd_json).select($"order_no", $"status", $"created_at",
          //            from_utc_timestamp($"created_at", "Asia/Shanghai").alias("created_at"),
          date_format($"created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
          date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("etl_ms")
        ).withColumn("oem", lit("jybk")).show(false)
        //          .write
        //          .mode("append")
        //          .format("parquet")
        //          .saveAsTable("demo.jybk_orders")
      }
    })
  }

}
