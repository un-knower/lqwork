package com.lqwork.mohe

import com.lqwork.common.SparkSessionInit

import scala.io.Source._

object OperatorJYBKReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionInit.initSession("get_jybk_report")

    spark.read.table("buffer.buffer_jybk_operator")
      .where("updated_at >= '2018-01-11 00:00:00'")
      .select("report_url").where(" report_url != 'null'")
      .rdd.repartition(20).mapPartitions(iter => {
      iter.map(m => {
        fromURL(m.get(0).toString.replace("-internal", ""), "utf-8").mkString
      })
    }).saveAsTextFile("/third-data/pgdoperator/jybk_v2/all/20180111")

    spark.close()
  }
}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.mohe.OperatorJYBKReport \
--master spark://10.1.11.61:7077 \
--executor-memory 10G \
--total-executor-cores 20 \
/opt/taf/jar/lqwork.jar
*/