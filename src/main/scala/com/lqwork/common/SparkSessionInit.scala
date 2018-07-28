package com.lqwork.common

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object SparkSessionInit {
  def initSession(appName: String): SparkSession ={
    val config = ConfigFactory.load()

    val spark = SparkSession
      .builder().appName(appName)
      .config("spark.driver.maxResultSize", "4g") // 每个Spark action(如collect)所有分区的序列化结果的总大小限制
      .config("spark.task.maxFailures", "8") // Task最大重试次数
      .config("spark.rpc.numRetries", "15")
      .config("spark.network.timeout", "600s")
      .config("spark.shuffle.file.buffer", "1024k")
      .config("spark.sql.shuffle.partitions", 300)
      .config("spark.reducer.maxSizeInFlight", "96m")
      .config("spark.sql.autoBroadcastJoinThreshold", 20L * 1024 * 1024)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.consolidateFiles", true)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }
}

//.config("spark.shuffle.memoryFraction", "0.3")
//.config("spark.shuffle.manager", "hash")
//.config("spark.shuffle.consolidateFiles", "true")
//.config("redis.host", config.getString("application.redis.host"))
//.config("redis.port", config.getString("application.redis.port"))
//.config("redis.auth", config.getString("application.redis.auth"))

//.config("spark.locality.wait", "20")
//.config("spark.task.maxFailures", "8")
//.config("spark.rpc.numRetries", "15")
//.config("spark.network.timeout", "600s")
//.config("spark.shuffle.io.retryWait", "100s")
//.config("spark.shuffle.io.maxRetries", "30")
//.config("spark.shuffle.file.buffer", "256k")
//.config("spark.reducer.maxSizeInFlight", "96m")