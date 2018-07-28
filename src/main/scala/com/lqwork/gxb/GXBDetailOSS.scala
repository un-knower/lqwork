package com.lqwork.gxb

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.lqwork.common.{SparkSessionInit, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.json.{JSONArray, JSONObject}

import scala.io.Source._

object GXBDetailOSS {
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
    val df_url = util.getGXBBaseURL(spark, oem).persist(StorageLevel.MEMORY_AND_DISK)

    // 电商数据
    val rdd_eco = readOSSParseEco(df_url, spark, oem, yesterday).persist(StorageLevel.MEMORY_AND_DISK)
//    spark.read.json(rdd_eco.map(_._1)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.ecommerceBaseInfo")
//    spark.read.json(rdd_eco.map(_._2)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.huabeiInfo")
//    if (!rdd_eco.map(_._3).isEmpty()) {
//      spark.read.json(rdd_eco.map(_._3)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.jiebeiInfo")
//    }
//    spark.read.json(rdd_eco.map(_._4)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.ecommerceConsigneeAddresses")
//    spark.read.json(rdd_eco.map(_._5)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.ecommerceTrades")
//    spark.read.json(rdd_eco.map(_._6)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.taobaoOrders")

    // 京东数据
    val rdd_jd = readOSSParseJd(df_url, spark, oem, yesterday).persist(StorageLevel.MEMORY_AND_DISK)
    spark.read.json(rdd_jd.map(_._1)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.accountInfo")
    spark.read.json(rdd_jd.map(_._2)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.certificationInfo")
    spark.read.json(rdd_jd.map(_._3)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.bankCardList")
    spark.read.json(rdd_jd.map(_._4)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.addressList")
    spark.read.json(rdd_jd.map(_._5)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.loginHistoryList")
    spark.read.json(rdd_jd.map(_._6)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.invoiceList")
    spark.read.json(rdd_jd.map(_._7)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.btBillList")
    spark.read.json(rdd_jd.map(_._8)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.btRepayInfoList")
    spark.read.json(rdd_jd.map(_._9)).withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").format("parquet").saveAsTable("taf.orderList")
  }

  def readOSSParseEco(df_url: Dataset[Row], spark: SparkSession, oem: String, yesterday: String): RDD[(String,String,String,String,String,String)] = {
    val broadcastVar = spark.sparkContext.broadcast(new JSONArray())
    val rdd_eco = df_url.where("auth_type = 'ecommerce'").rdd.repartition(40).mapPartitions(iter => {
      iter.map(m => {
        val user_id = m.get(0).toString
        val idcard = m.get(1).toString
        val report_addr = m.get(2).toString
        val updated_at = m.get(3).toString
        val report_type_id = m.get(4)

        var json = new JSONObject(fromURL(report_addr, "utf-8").mkString)
        if (report_type_id == 2) {
          json = json.getJSONObject("data").getJSONObject("authResult")
        }
        // 用户基本信息
        val ecommerceBaseInfo = json.getJSONObject("ecommerceBaseInfo")
        // 花呗信息
        val huabeiInfo = json.getJSONObject("huabeiInfo")
        // 借呗信息
        val jiebeiInfo = json.getJSONObject("jiebeiInfo")
        // 收货地址
        var ecommerceConsigneeAddresses = broadcastVar.value
        if(json.has("ecommerceConsigneeAddresses")){
          ecommerceConsigneeAddresses = json.getJSONArray("ecommerceConsigneeAddresses")
        } else {
          println(user_id)
        }
        // 交易记录
        var ecommerceTrades = broadcastVar.value
        if(json.has("ecommerceTrades")){
          ecommerceTrades = json.getJSONArray("ecommerceTrades")
        } else {
          println(user_id)
        }
        // 淘宝交易记录
        var taobaoOrders = broadcastVar.value
        if(json.has("taobaoOrders")){
          taobaoOrders = json.getJSONArray("taobaoOrders")
        } else {
          println(user_id)
        }

        util.jsonObjectAddKV(ecommerceBaseInfo, user_id, idcard, updated_at)
        util.jsonObjectAddKV(huabeiInfo, user_id, idcard, updated_at)
        util.jsonObjectAddKV(jiebeiInfo, user_id, idcard, updated_at)
        util.jsonArrayAddKV(ecommerceConsigneeAddresses, user_id, idcard, updated_at)
        util.jsonArrayAddKV(ecommerceTrades, user_id, idcard, updated_at)
        util.jsonArrayAddKV(taobaoOrders, user_id, idcard, updated_at)
        (ecommerceBaseInfo.toString(),huabeiInfo.toString(),jiebeiInfo.toString(),
          ecommerceConsigneeAddresses.toString(),ecommerceTrades.toString(),taobaoOrders.toString())
      })
    })

    rdd_eco
  }

  def readOSSParseJd(df_url: Dataset[Row], spark: SparkSession, oem: String, yesterday: String): RDD[(String,String,String,String,String,String,String,String,String)] = {
    val rdd_jd = df_url.where("auth_type = 'jd'").rdd.repartition(40).mapPartitions(iter => {
      iter.map(m => {
        val user_id = m.get(0).toString
        val idcard = m.get(1).toString
        val report_addr = m.get(2).toString
        val updated_at = m.get(3).toString
        val report_type_id = m.get(4)

        var json = new JSONObject(fromURL(report_addr, "utf-8").mkString)
        if (report_type_id == 2) {
          json = json.getJSONObject("data").getJSONObject("authResult")
        }
        // 账户信息
        val accountInfo = json.getJSONObject("accountInfo")
        // 认证信息
        val certificationInfo = json.getJSONObject("certificationInfo")
        // 绑卡信息
        val bankCardList = json.getJSONArray("bankCardList")
        // 地址信息
        val addressList = json.getJSONArray("addressList")
        // 登陆信息
        val loginHistoryList = json.getJSONArray("loginHistoryList")
        // 发票信息
        val invoiceList = json.getJSONArray("invoiceList")
        // 白条账单信息
        val btBillList = json.getJSONArray("btBillList")
        // 白条还款信息
        val btRepayInfoList = json.getJSONArray("btRepayInfoList")
        // 购物订单信息
        val orderList = json.getJSONArray("orderList")

        util.jsonObjectAddKV(accountInfo, user_id, idcard, updated_at)
        util.jsonObjectAddKV(certificationInfo, user_id, idcard, updated_at)
        util.jsonArrayAddKV(bankCardList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(addressList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(loginHistoryList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(invoiceList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(btBillList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(btRepayInfoList, user_id, idcard, updated_at)
        util.jsonArrayAddKV(orderList, user_id, idcard, updated_at)
        (accountInfo.toString(),certificationInfo.toString(),bankCardList.toString(),addressList.toString(),loginHistoryList.toString(),
          invoiceList.toString(),btBillList.toString(),btRepayInfoList.toString(),orderList.toString())
      })
    })
    rdd_jd
  }
}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.gxb.GXBDetailOSS \
--master spark://10.1.11.61:7077 \
--executor-memory 10G \
--total-executor-cores 20 \
/opt/taf/jar/lqwork.jar \
1 \
sdhh
*/