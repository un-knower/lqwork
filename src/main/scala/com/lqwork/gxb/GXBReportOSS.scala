package com.lqwork.gxb

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.lqwork.common.{SparkSessionInit, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.json.{JSONArray, JSONObject}

import scala.io.Source._

object GXBReportOSS {
  private val util = new Utils()

  def main(args: Array[String]): Unit = {
    val yesterday = LocalDate.now().minusDays(args(0).toInt).toString
    val oem = args(1)
    val spark = SparkSessionInit.initSession("gxh_" + oem)

    mapTable(spark, oem, yesterday)

    spark.close()
  }

  def mapTable(spark: SparkSession, oem: String, yesterday: String): Unit = {
    val etl_ms = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val rdd = readOSSParse(spark, oem, yesterday).persist(StorageLevel.MEMORY_AND_DISK)

    spark.read.json(rdd.map(_._1)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.baseInfo")
    spark.read.json(rdd.map(_._2)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.assetsInfo")
    spark.read.json(rdd.map(_._3)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.behaviorInfo")
    spark.read.json(rdd.map(_._4)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.taobaoAddressList")
    spark.read.json(rdd.map(_._5)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.taobaoShopList")
    spark.read.json(rdd.map(_._6)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.repaymentList")
    spark.read.json(rdd.map(_._7)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.consumptionList")
    spark.read.json(rdd.map(_._8)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("taf.incomeList")
  }

  def readOSSParse(spark: SparkSession, oem: String, yesterday: String): RDD[(String,String,String,String,String,String,String,String)] = {
    val df_url = util.getGXBReportURL(spark, oem)
    val bc_obj = spark.sparkContext.broadcast(new JSONObject())
    val bc_arr = spark.sparkContext.broadcast(new JSONArray())
    df_url.rdd.repartition(40).mapPartitions(iter => {
      iter.map(m => {
        val user_id = m.get(0).toString
        val idcard = m.get(1).toString
        val report_final_url = m.get(2).toString
        val updated_at = m.get(3).toString

        val json = new JSONObject(fromURL(report_final_url, "utf-8").mkString)
          .getJSONObject("data").getJSONObject("authResult")
        var reportSummary = bc_obj.value
        var baseInfo = bc_obj.value
        var assetsInfo = bc_obj.value
        var behaviorInfo = bc_obj.value
        var taobaoAddressList = bc_arr.value
        var taobaoShopList = bc_arr.value
        if (json.has("reportSummary")) {
          reportSummary = json.getJSONObject("reportSummary")
          baseInfo = reportSummary.getJSONObject("baseInfo")
          assetsInfo = reportSummary.getJSONObject("assetsInfo")
          behaviorInfo = reportSummary.getJSONObject("behaviorInfo")
          taobaoAddressList = reportSummary.getJSONArray("taobaoAddressList")
          taobaoShopList = reportSummary.getJSONArray("taobaoShopList")
        }

        var expenditureReport = bc_obj.value
        var repaymentList = bc_arr.value
        var consumptionList = bc_arr.value
        if (json.has("expenditureReport")) {
          expenditureReport= json.getJSONObject("expenditureReport")
          repaymentList = expenditureReport.getJSONArray("repaymentList")
          consumptionList = expenditureReport.getJSONArray("consumptionList")
        }

        var incomeReport = bc_obj.value
        var incomeList = bc_arr.value
        if (json.has("incomeReport")) {
          incomeReport= json.getJSONObject("incomeReport")
          incomeList = incomeReport.getJSONArray("incomeList")
        }

        util.jsonObjectAddKV(baseInfo, user_id, idcard, updated_at)
        util.jsonObjectAddKV(assetsInfo, user_id, idcard, updated_at)
        util.jsonObjectAddKV(behaviorInfo, user_id, idcard, updated_at)
        util.jsonArrayAddKV(taobaoAddressList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(taobaoShopList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(repaymentList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(consumptionList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(incomeList, user_id, idcard, updated_at)

        (baseInfo.toString,assetsInfo.toString,behaviorInfo.toString,taobaoAddressList.toString,
          taobaoShopList.toString,repaymentList.toString,consumptionList.toString,incomeList.toString)
      })
    })
  }

}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.gxb.GXBReportOSS \
--master spark://10.1.11.61:7077 \
--executor-memory 10G \
--total-executor-cores 20 \
/opt/taf/jar/lqwork.jar \
1 \
sdhh
*/