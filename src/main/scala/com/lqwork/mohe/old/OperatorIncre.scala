package com.lqwork.mohe.old

import java.time.LocalDate

import com.lqwork.common.SparkSessionInit

object OperatorIncre {
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

    //每天新增taskid
    val task_ids = spark.sql("select concat(user_id, '^', task_id, '^', updated_at) from buffer.buffer_"+ oem +"_operator where updated_at like '" + occur_time + "%' ")
      .withColumnRenamed("updated_at", "operator_auth_date").collect()
//    val task_ids = spark.sql("select concat(user_id, '^', task_id, '^', updated_at) from buffer.buffer_"+ oem +"_operator where updated_at < '" + time +"'")
//      .withColumnRenamed("updated_at", "operator_auth_date").collect()

//    //通过taskid请求拉取运营商通话记录等数据, 并写入到HDFS
//    GetData.getOperateData(spark, occur_time, task_ids, operHDFSPath)

    //读取HDFS上的json数据，并解析返回json类型的RDD
    val operDataRDD = JsonParse.getOperateJson(spark, operHDFSPath, occur_time)
    //将json类型的RDD映射成表写入Hive(运营商通话记录)
    WriteIntoHive.writeOperToHive(spark, occur_time, oem, operDataRDD)

    spark.close()
  }
}
///home/hadoop/app/spark-2.1.0/bin/spark-submit --class com.lq.append.OperatorIncre --master spark://10.1.11.61:7077 --executor-memory 15G --total-executor-cores 20 /opt/taf/pgd.jar 25 /third-data/pgdoperator/baobao_v4/baobao_operate_ /third-data/pgdoperator/baobao_v2/baobao_finance_ baobao