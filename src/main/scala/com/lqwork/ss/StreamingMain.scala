package com.lqwork.ss

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingMain {
  def main(args: Array[String]): Unit = {
    //Initializing SparkContext and StreamingContext
    val spark = SparkSession.builder().appName("StreamingMain").master("local[4]").getOrCreate()
//    val spark = SparkSession.builder().appName("StreamingMain").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val config = ConfigFactory.load()
    val ssc = new StreamingContext(sc, Seconds(config.getInt("application.spark_batch_interval")))

    SaveData.saveToHive(spark, config, ssc)

    ssc.start()
    ssc.awaitTermination()
  }
}
/*

/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.sparkstreaming.StreamingMain \
--master spark://10.1.11.61:7077 \
--executor-memory 5G \
--total-executor-cores 10 \
/opt/taf/lqwork.jar

/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.sparkstreaming.StreamingMain \
--master yarn \
--deploy-mode cluster \
--executor-memory 5G \
--num-executors 10 \
/opt/taf/lqwork.jar

*/