package com.lqwork.mohe

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

import com.lqwork.common.{Utils, SparkSessionInit}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.storage.StorageLevel
import org.json.JSONObject

import scala.io.Source.fromURL

object OperatorReportOSS {
  private val util = new Utils()

  def main(args: Array[String]): Unit = {
    val yesterday = LocalDate.now().minusDays(args(0).toInt).toString
    val oem = args(1)
    val spark = SparkSessionInit.initSession("operator_" + oem)

    mapTable(spark, oem, yesterday)

    spark.close()
  }

  def mapTable(spark: SparkSession, oem: String, yesterday: String): Unit = {
    val etl_ms = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val rdd = readOSSParse(spark, oem, yesterday).persist(StorageLevel.MEMORY_AND_DISK)

    // Save in table.
    spark.read.json(rdd.map(_._1)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_user_info")
    spark.read.json(rdd.map(_._2)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_mobile_info")
    spark.read.json(rdd.map(_._3)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_active_silence_stats")
    spark.read.json(rdd.map(_._4)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_all_contact_stats")
    spark.read.json(rdd.map(_._5)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_behavior_analysis")
    spark.read.json(rdd.map(_._6)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_behavior_score")
    spark.read.json(rdd.map(_._7)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_carrier_consumption_stats")
    spark.read.json(rdd.map(_._8)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_contact_blacklist_analysis")
    spark.read.json(rdd.map(_._9)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_contact_creditscore_analysis")
    spark.read.json(rdd.map(_._10)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_contact_manyheads_analysis")
    spark.read.json(rdd.map(_._11)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_finance_contact_detail")
    spark.read.json(rdd.map(_._12)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_finance_contact_stats")
    spark.read.json(rdd.map(_._13)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_risk_contact_detail")
    spark.read.json(rdd.map(_._14)).withColumn("etl_ms", lit(etl_ms)).coalesce(5).write.mode("overwrite").format("parquet").saveAsTable("operator."+ oem +"_risk_contact_stats")
  }

  def readOSSParse(spark: SparkSession, oem: String, yesterday: String): RDD[(String,String,String,String,String,String,String,String,String,String,String,String,String,String)] ={
    val df_fin = util.getReportURL(spark, oem)
    df_fin.rdd.repartition(40).mapPartitions(iter => {
      iter.map(m => {
        val user_id = m.get(0).toString
        val report_url = m.get(1).toString
        val updated_at = m.get(2).toString

        val json = new JSONObject(fromURL(report_url, "utf-8").mkString)
        val data = json.getJSONObject("data")
        // 用户信息
        val user_info = data.getJSONObject("user_info")
        // 手机信息
        val mobile_info = data.getJSONObject("mobile_info")
        mobile_info.remove("identity_code")

        val identity_code = user_info.getString("identity_code") // 身份证号

        // 静默活跃统计
        val active_silence_stats = data.getJSONObject("active_silence_stats")
        // 全部联系人统计
        val all_contact_stats  = data.getJSONObject("all_contact_stats")
        // 行为分析
        val behavior_analysis = data.getJSONObject("behavior_analysis")
        // 行为评分
        val behavior_score = data.getJSONObject("behavior_score")
        // 运营商消费统计
        val carrier_consumption_stats = data.getJSONObject("carrier_consumption_stats")
        // 联系人黑名单分析
        val contact_blacklist_analysis = data.getJSONObject("contact_blacklist_analysis")
        // 联系人智信分分析
        val contact_creditscore_analysis = data.getJSONObject("contact_creditscore_analysis")
        // 联系人多平台申请分析
        val contact_manyheads_analysis = data.getJSONObject("contact_manyheads_analysis")
        // 金融机构联系人明细
        val finance_contact_detail = data.getJSONArray("finance_contact_detail")
        // 金融机构联系人统计
        val finance_contact_stats = data.getJSONArray("finance_contact_stats")
        // 风险联系人明细
        val risk_contact_detail = data.getJSONArray("risk_contact_detail")
        // 风险联系人统计
        val risk_contact_stats = data.getJSONArray("risk_contact_stats")

        user_info.putOnce("user_id", user_id).putOnce("updated_at", updated_at)
        mobile_info.putOnce("user_id", user_id).putOnce("identity_code", identity_code).putOnce("updated_at", updated_at)
        util.jsonObjectAddKV(active_silence_stats,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(all_contact_stats,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(behavior_analysis,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(behavior_score,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(carrier_consumption_stats,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(contact_blacklist_analysis,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(contact_creditscore_analysis,user_id,identity_code,updated_at)
        util.jsonObjectAddKV(contact_manyheads_analysis,user_id,identity_code,updated_at)
        util.jsonArrayAddKV(finance_contact_detail,user_id,identity_code,updated_at)
        util.jsonArrayAddKV(finance_contact_stats,user_id,identity_code,updated_at)
        util.jsonArrayAddKV(risk_contact_detail,user_id,identity_code,updated_at)
        util.jsonArrayAddKV(risk_contact_stats,user_id,identity_code,updated_at)

        (user_info.toString,mobile_info.toString,active_silence_stats.toString,all_contact_stats.toString,behavior_analysis.toString,behavior_score.toString,
          carrier_consumption_stats.toString,contact_blacklist_analysis.toString,contact_creditscore_analysis.toString,contact_manyheads_analysis.toString,
          finance_contact_detail.toString,finance_contact_stats.toString,risk_contact_detail.toString,risk_contact_stats.toString)
      })
    })
  }
}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.mohe.OperatorReportOSS \
--master spark://10.1.11.61:7077 \
--executor-memory 10G \
--total-executor-cores 20 \
/opt/taf/jar/lqwork.jar \
1 \
lxb
*/