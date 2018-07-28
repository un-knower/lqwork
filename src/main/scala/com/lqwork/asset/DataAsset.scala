package com.lqwork.asset

import com.lqwork.common.SparkSessionInit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DataAsset {
  def main(args: Array[String]): Unit = {
    val oem = args(0)
    val spark = SparkSessionInit.initSession(oem + "Asset")

    val arr = Array("baobao","msdq","jybk","yrd","xzbk","xzbk2","aj","ddh","ddhs","hjh","hjsj","jm","yhd","ymbk","sdhh","kb","bb","lxb","lxbh5")
    for (oem <- arr) {
      aggAsset(spark, oem)
        .withColumn("oem", lit(oem))
        .write.mode("append").format("parquet").saveAsTable("")
    }

  }

  def aggAsset(spark: SparkSession, oem: String): DataFrame ={
    import spark.implicits._

    //用户手机号
    val df_ZC001 = spark.read.table("buffer_" + oem + "_users").select($"phone", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("phone").as("phone"))
    //非用户手机号
    val df_ZC002 = spark.read.table("")
    //身份证
    val df_ZC003 = spark.read.table("buffer_" + oem + "_cards").select($"idcard", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("idcard").as(""))
    //通讯录
    val df_ZC004 = spark.read.table("")
    //运营商报告
    val df_ZC005 = spark.read.table("buffer_" + oem + "_operator").select($"report_url", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("").as(""))
    //收货地址
    val df_ZC006 = spark.read.table("buffer_" + oem + "_addr").select($"detail_addr", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("detail_addr").as(""))
    //定位信息
    val df_loc = spark.read.table("buffer_extend_icloud_locations").select($"addr", $"updated_at", $"app_id")
    val df_app = spark.read.table("buffer_extend_app_configs").select($"app", $"id".alias("app_id"))
    val df_ZC007 = df_loc.join(df_app, Seq("app_id"), "left_outer").where("app = '" + oem + "'")
      .select($"addr", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("addr").as(""))
    //借记卡
    val df_ZC008 = spark.read.table("buffer_" + oem + "_banks").select($"card", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("card").as(""))
    //信用卡
    val df_ZC009 = spark.read.table("")
    //同盾贷前审核报告
    val df_ZC010 = spark.read.table("buffer_" + oem + "_tongdun_reports").select($"data", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("data").as(""))
    //有盾报告
    val df_ZC011 = spark.read.table("buffer_" + oem + "_youdun_report").select($"report_url", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("report_url").as(""))
    //贷后帮报告
    val df_ZC012 = spark.read.table("ods.ods_dhb_detail").select("")
    //公信宝报告
    val df_ZC013 = spark.read.table("buffer_" + oem + "_gxb_authorizations").select($"", to_date($"updated_at").as("everyday"))
      .groupBy("everyday").agg(count("").as(""))
    //订单申请数量
    val df_ZC014 = spark.read.table("buffer_" + oem + "_orders").select($"order_no", to_date($"created_at").as("everyday"))
      .groupBy("everyday").agg(count("order_no").as("order_no"))
    //催收记录数
    val df_col = spark.read.table("buffer_" + oem + "_tb_collection").select($"collection_id", $"project_id", $"modify_time")
    val df_pro = spark.read.table("buffer_" + oem + "_tb_collection").select($"project_id", $"project_code")
    val df_ZC015 = df_col.join(df_pro, Seq("project_id"), "left_outer").where("project_code = '" + oem + "'")
      .select($"collection_id", to_date($"modify_time").as("everyday"))
      .groupBy("everyday").agg(count("collection_id").as("collection_id"))
    //黑名单用户数量
    val df_ZC016 = spark.read.table("buffer_" + oem + "_bl_user").where("app_id = '" + oem + "'")
      .select($"idcard_no", to_date($"modify_time").as("everyday"))
      .groupBy("everyday").agg(count("idcard_no").as("ZC016"))


    df_ZC001.join(df_ZC002, Seq("everyday"), "left_outer")
      .join(df_ZC003, Seq("everyday"), "left_outer")
      .join(df_ZC004, Seq("everyday"), "left_outer")
      .join(df_ZC005, Seq("everyday"), "left_outer")
      .join(df_ZC006, Seq("everyday"), "left_outer")
      .join(df_ZC007, Seq("everyday"), "left_outer")
      .join(df_ZC008, Seq("everyday"), "left_outer")
      .join(df_ZC009, Seq("everyday"), "left_outer")
      .join(df_ZC010, Seq("everyday"), "left_outer")
      .join(df_ZC011, Seq("everyday"), "left_outer")
      .join(df_ZC012, Seq("everyday"), "left_outer")
      .join(df_ZC013, Seq("everyday"), "left_outer")
      .join(df_ZC014, Seq("everyday"), "left_outer")
      .join(df_ZC015, Seq("everyday"), "left_outer")
      .join(df_ZC016, Seq("everyday"), "left_outer")

  }
}
