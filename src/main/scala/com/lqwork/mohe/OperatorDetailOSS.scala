package com.lqwork.mohe

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.lqwork.common.{Utils, SparkSessionInit}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel
import org.json.{JSONArray, JSONObject}

import scala.io.Source._

object OperatorDetailOSS {
//  val spark = SparkSessionInit.initSession("operator")
//  import spark.implicits._
  private val util = new Utils()

  def main(args: Array[String]): Unit = {
    val yesterday = LocalDate.now().minusDays(args(0).toInt).toString
    val oem = args(1)
    val spark = SparkSessionInit.initSession("operator_" + oem)

    mapTable(spark, oem, yesterday)

    spark.close()
  }

  def mapTable(spark: SparkSession, oem: String, yesterday: String): Unit ={
    val rdd = readOSSParse(spark, oem, yesterday).persist(StorageLevel.MEMORY_AND_DISK)
    val etl_ms = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    spark.read.json(rdd.map(_._1)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_base_info")
    spark.read.json(rdd.map(_._2)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_account_info")
    spark.read.json(rdd.map(_._3)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_bill_info")
    spark.read.json(rdd.map(_._4)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_sms_info")
    spark.read.json(rdd.map(_._5)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_call_info")
    spark.read.json(rdd.map(_._6)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_payment_info")
    spark.read.json(rdd.map(_._7)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_bill_record")
    spark.read.json(rdd.map(_._8)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_sms_record")
    spark.read.json(rdd.map(_._9)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_call_record")
  }

  def readOSSParse(spark: SparkSession, oem: String, yesterday: String): RDD[(String, String, String, String, String, String,String, String, String)] ={
    val df_oper = util.getBaseURL(spark, oem)
    df_oper.rdd.repartition(40).mapPartitions(iter => {
      iter.map(m => {
        val user_id = m.get(0).toString
        val base_url = m.get(1).toString
        val updated_at = m.get(2).toString

        val json = new JSONObject(fromURL(base_url, "utf-8").mkString)
        val data = json.getJSONObject("data")
        val task_data = data.getJSONObject("task_data")
        // 身份证
        val identity_code = data.getString("identity_code")

        // 个人信息
        val base_info = task_data.getJSONObject("base_info")
        // 账户信息
        val account_info = task_data.getJSONObject("account_info")
        // 账单信息
        val bill_info = task_data.getJSONArray("bill_info")
        // 短信详单
        val sms_info = task_data.getJSONArray("sms_info")
        // 通话详单
        val call_info = task_data.getJSONArray("call_info")
        // 缴费信息
        val payment_info = task_data.getJSONArray("payment_info")

        util.jsonObjectAddKV(base_info,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(account_info,user_id,identity_code,updated_at)
        val bill_record = jsonNestArrayAddKV(bill_info,"bill_record", "bill_cycle",user_id,identity_code,updated_at)
        val sms_record = jsonNestArrayAddKV(sms_info,"sms_record", "msg_cycle",user_id,identity_code,updated_at)
        val call_record = jsonNestArrayAddKV(call_info,"call_record", "call_cycle",user_id,identity_code,updated_at)
        util.jsonArrayAddKV(payment_info,user_id,identity_code,updated_at)

        //(base_info, account_info, bill_info, sms_info, call_info, payment_info, bill_record, sms_record, call_record)
        (base_info.toString, account_info.toString, bill_info.toString, sms_info.toString, call_info.toString, payment_info.toString, bill_record.toString, sms_record.toString, call_record.toString)
      })
    })
  }

  def jsonNestArrayAddKV(infoArr: JSONArray, record: String, cycleKey: String, user_id: String, identity_code:String, updated_at: String): JSONArray ={
    //val infoArray = new JSONArray()
    val recordArr = new JSONArray()
    val len = infoArr.length()
    if (len > 0) {
      for (i <- 0 until len) {
        val infoObj = infoArr.getJSONObject(i)
        val cycleValue = infoObj.getString(cycleKey) // 账单周期, 通话周期, 短信周期(YYYY-MM)
        val recArr = infoObj.getJSONArray(record)
        for (j <- 0 until recArr.length()) {
          val inner = recArr.getJSONObject(j)
          util.jsonObjectAddKV(inner, user_id, identity_code, updated_at)
            .putOnce(cycleKey, cycleValue) // put之后, inner结构已经改变
          recordArr.put(inner)
        }

        infoObj.remove(record) // 调用remove接口移除kv后, infoArr的结构会随着发生变化
        val outer = infoArr.getJSONObject(i)
        util.jsonObjectAddKV(outer, user_id, identity_code, updated_at)
      }
    }
    recordArr
  }

}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.mohe.OperatorDetailOSS \
--master spark://10.1.11.61:7077 \
--executor-memory 10G \
--total-executor-cores 20 \
/opt/taf/jar/lqwork.jar \
1 \
lxb
*/