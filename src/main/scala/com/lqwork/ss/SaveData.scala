package com.lqwork.ss

import java.util.Properties

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import org.apache.spark.streaming.StreamingContext
import org.json.JSONObject

object SaveData {
  private var json = new JSONObject()

  def singleJson(jsonStr: String) ={
    new JSONObject(jsonStr)
  }

  def saveToHive(spark: SparkSession, config: Config, ssc: StreamingContext): Unit ={
    val messages = CustomDirectStream.createCustomDirectKafkaStream(spark, config, ssc)

    val url = config.getString("application.url")
    val table = config.getString("application.table")

    val prop = new Properties()
    prop.put("username", config.getString("application.username"))
    prop.put("password", config.getString("application.password"))

    val sc = spark.sparkContext

    //Processing json and save into Hive table.
    import spark.implicits._
    messages.map(_._2).foreachRDD(rdd => {
      if (rdd.isEmpty()) {
        println("warning: no kafka data")
      } else {
        /*val rdd_json = rdd.map(m => {
          val json = new JSONObject(m)
          val ts_ms = json.get("ts_ms").toString
          val after = json.getJSONObject("after")
          after.toString()
        })

        spark.read.json(rdd_json).show(false)
        val df_final = spark.read.json(rdd_json)
          .select($"order_no", $"total_price", $"onhire_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("etl_ms"))
          .withColumn("oem", lit("lianji"))
        df_final.show(false)*/

        val rdd_json_rent = rdd.filter(f => {
          val j = singleJson(f)
          val source = j.getJSONObject("source")
          "order_rent".equals(source.getString("table"))
        }).map(m => {
          val j = singleJson(m)
          val after = j.getJSONObject("after")
          after.toString()
        })

        spark.read.json(rdd_json_rent).show(false)
        spark.read.json(rdd_json_rent)
          .select($"order_no", $"total_price", $"onhire_time", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("etl_ms"))
          .withColumn("oem", lit("lianji")).show(false)

        val rdd_json_sale = rdd.filter(f => {
          val j = singleJson(f)
          val source = j.getJSONObject("source")
          "order_rent".equals(source.getString("table"))
        }).map(m => {
          val j = singleJson(m)
          val after = j.getJSONObject("after")
          after.toString()
        })

        spark.read.json(rdd_json_sale).show(false)
        spark.read.json(rdd_json_sale)
          .select($"order_no", $"total_price", date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("etl_ms"))
          .withColumn("oem", lit("lianji")).show(false)
        //save to Hive
//        df_final.write.mode("append").format("parquet").saveAsTable("demo.order_rent")

        /*val df_final = spark.read.json(rdd_json).select($"order_no", $"status",
          date_format($"created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
          date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("etl_ms")
        ).withColumn("oem", lit("jybk")).show()*/
/*        val df_final = spark.read.json(rdd_json).select($"order_no", $"status",
          date_format($"created_at", "yyyy-MM-dd HH:mm:ss").alias("created_at"),
          date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss").alias("etl_ms")
        ).withColumn("oem", lit("jybk"))

        //save to Hive
        df_final.write.mode("append").format("parquet").saveAsTable("demo.jybk_orders")

        //save to MySQL
        df_final.write.mode("append").format("parquet").jdbc(url, "", prop)

        //save to Redis
        import com.redislabs.provider.redis._
//        sc.toRedisKV()*/

      }
    })
  }
}