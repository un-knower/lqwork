package com.lqwork.gxb

import java.time.LocalDate

import com.lqwork.common.SparkSessionInit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FixGXB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionInit.initSession("fix_gxb")

//    fixDetail(spark, "sdhh")
    spark.read.table("operator.aj_finance_contact_detail").show(1)
    spark.close()
  }

  def fixDetail(spark: SparkSession, oem: String): Unit ={
    val etl_ms = LocalDate.now().minusDays(1).toString
    // ecommerce
    spark.read.table("gxb."+ oem +"_ecommercebaseinfo_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_ecommercebaseinfo")
    spark.read.table("gxb."+ oem +"_ecommerceconsigneeaddresses_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_ecommerceconsigneeaddresses")
    spark.read.table("gxb."+ oem +"_huabeiinfo_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_huabeiinfo")
    spark.read.table("gxb."+ oem +"_ecommercetrades_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .groupBy("user_id","idCard","amount","isDelete","otherSide","title","tradeDetailUrl","tradeNo","tradeStatusName","tradeTime","txTypeId","txTypeName")
      .agg(min("updated_at").alias("updated_at"))
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_ecommercetrades")
    spark.read.table("gxb."+ oem +"_taobaoorders_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .groupBy("user_id","idCard","actualFee","address","createTime","endTime","invoiceName","logistics","orderNumber","payTime","postFees","seller","subOrders","totalQuantity","tradeNumber","tradeStatusName","virtualSign")
      .agg(min("updated_at").alias("updated_at"))
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_taobaoorders")


    // jd
    spark.read.table("gxb."+ oem +"_accountinfo_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_accountinfo")
    spark.read.table("gxb."+ oem +"_addresslist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_addresslist")
    spark.read.table("gxb."+ oem +"_bankcardlist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_bankcardlist")
    spark.read.table("gxb."+ oem +"_btbilllist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_btbilllist")
    spark.read.table("gxb."+ oem +"_btrepayinfolist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_btrepayinfolist")
    spark.read.table("gxb."+ oem +"_certificationinfo_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_certificationinfo")
    spark.read.table("gxb."+ oem +"_invoicelist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_invoicelist")
    spark.read.table("gxb."+ oem +"_loginhistorylist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_loginhistorylist")
    spark.read.table("gxb."+ oem +"_orderlist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .groupBy("address","amount","btInterestNum","created_at","idCard","isBtOverDue","isBtPay","isBtRepay","itemList","orderStatus","orderStatusName","otherSide","payWay","phone","receiveName","tradeNo","tradeTime","user_id")
      .agg(min("updated_at").alias("updated_at"))
      .withColumn("etl_ms", lit(etl_ms)).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_orderlist ")
  }

  def fixReport(spark: SparkSession, oem: String): Unit ={
    // report
    spark.read.table("gxb."+ oem +"_baseinfo_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_baseinfo")
    spark.read.table("gxb."+ oem +"_assetsinfo_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_assetsinfo")
    spark.read.table("gxb."+ oem +"_taobaoaddresslist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_taobaoaddresslist")
    spark.read.table("gxb."+ oem +"_taobaoshoplist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_taobaoshoplist")
    spark.read.table("gxb."+ oem +"_behaviorinfo_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_behaviorinfo")
    spark.read.table("gxb."+ oem +"_incomelist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_incomelist")
    spark.read.table("gxb."+ oem +"_consumptionlist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_consumptionlist")
    spark.read.table("gxb."+ oem +"_repaymentlist_tmp").drop("created_at").withColumnRenamed("idCard", "identity_code")
      .withColumn("etl_ms", lit("2018-07-09")).repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("gxb."+ oem +"_repaymentlist")
  }
}
/*
/app/spark/bin/spark-submit \
--class com.lqwork.gxb.FixGXB \
--master yarn \
--deploy-mode cluster \
--executor-memory 4G \
--num-executors 20 \
/home/bigdata/taf/jars/lqwork.jar
*/