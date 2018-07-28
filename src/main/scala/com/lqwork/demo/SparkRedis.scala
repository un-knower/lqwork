package com.lqwork.demo

import com.lqwork.common.SparkSessionInit
import com.redislabs.provider.redis._

object SparkRedis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionInit.initSession("SparkRedis")
    val sc = spark.sparkContext

    val keys = Array[String]("key01", "key02")
    sc.fromRedisKV(keys).coalesce(1).foreach(println)

    val data = Seq[(String, String)](("key03", "Spark write redis"), ("key04", "redis test"))
    val rdd_redis = sc.parallelize(data)
    sc.toRedisKV(rdd_redis)

    spark.close()
  }
}
//https://github.com/RedisLabs/spark-redis
//http://shiyanjun.cn/archives/1097.html
///home/hadoop/app/spark-2.1.0/bin/spark-submit --class com.lqwork.demo.SparkRedis --master spark://10.1.11.61:7077 --executor-memory 5G --total-executor-cores 10 /opt/taf/lqwork.jar
///home/hadoop/app/spark-2.1.0/bin/spark-submit --class com.lqwork.demo.SparkRedis --master yarn --deploy-mode cluster  --executor-memory 5G --num-executors 10 /opt/taf/lqwork.jar