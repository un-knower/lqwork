package com.lqwork.gxb.detail

import java.time.LocalDate

import com.lqwork.common.SparkSessionInit

object Main {
  def main(args: Array[String]): Unit = {
    val yesterday = LocalDate.now().minusDays(args(0).toInt).toString
    val oem = args(1)

    val spark = SparkSessionInit.initSession("gxb_" + oem)

    //Request json data from oss, and save into Hive table.
    new RequestSaveToHive().requestAndSave(spark, oem, yesterday)

    val p = new ParseAndSave()
    p.ecommerce(spark, oem, yesterday)
    p.jd(spark, oem, yesterday)

    spark.close()
  }
}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
 --class com.lqwork.gxb.main.Main \
 --master spark://10.1.11.61:7077 \
 --executor-memory 5G \
 --total-executor-cores 10 \
 /opt/taf/jar/lqwork.jar \
 22 \
 lxb
 */