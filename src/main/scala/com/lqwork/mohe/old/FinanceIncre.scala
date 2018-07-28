package com.lqwork.mohe.old

import java.time.LocalDate

import com.lqwork.common.SparkSessionInit

object FinanceIncre {
  def main(args: Array[String]): Unit = {
    //提交spark应用时通过main函数传参数
    val yesterday    = args(0).toInt
    val operHDFSPath = args(1)
    val finHDFSPath  = args(2)
    val oem          = args(3)

    //初始化SparkSession
    val spark = SparkSessionInit.initSession(oem)
    //记录更新时间
    val occur_time = LocalDate.now().minusDays(yesterday).toString
//    val time = "2018-04-16 00:00:00"
//    val occur_time = "2018-04-15"
//    val task_ids = spark.sql("select concat(user_id, '^', task_id, '^', updated_at) from buffer.buffer_"+ oem +"_operator where updated_at < '" + time +"'")
//      .withColumnRenamed("updated_at", "operator_auth_date").collect()


    //每天新增taskid
    val task_ids = spark.sql("select concat(user_id, '^', task_id, '^', updated_at) from buffer.buffer_"+ oem +"_operator where updated_at like '" + occur_time + "%' ")
      .withColumnRenamed("updated_at", "operator_auth_date").collect()

    //通过taskid请求拉取金融、风险联系人等数据, 并写入到HDFS
    GetData.getFinanceData(task_ids, spark, occur_time, finHDFSPath)

    //读取HDFS上的json数据，并解析返回json类型的RDD
    val finDataRDD = JsonParse.getFinanceJson(spark, finHDFSPath, occur_time)
    //将json类型的RDD映射成表写入Hive(金融、风险联系人)
    WriteIntoHive.writeFinToHive(spark, occur_time, oem, finDataRDD)

    spark.close()
  }
}
