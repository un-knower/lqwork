package com.lqwork.mohe

import com.lqwork.common.SparkSessionInit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object FixOperator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionInit.initSession("fix_operator")

//    fixDetailAfter(spark, "sdhhh5")
//    fixDetail(spark, "aj")

//    fixReport(spark, "xzbk2")
//    fixReport(spark, "aj")
//    fixReport(spark, "ddh")
//    fixReport(spark, "hjh")
//    fixReport(spark, "ymbk")
//    fixReport(spark, "sdhh")
//    fixReport(spark, "kb")
//    fixReport(spark, "bb")
//    fixReport(spark, "lxb")
//    fixReport(spark, "lxbh5")
//    fixReport(spark, "lxb2")
//    fixReport(spark, "sdhhh5")

    spark.close()
  }

  def fixDetail(spark: SparkSession, oem: String): Unit ={
    import spark.implicits._
    val df_oper = spark.read.table("buffer.buffer_"+ oem +"_operator").select("user_id", "updated_at").distinct()
    val df_card = spark.read.table("buffer.buffer_"+ oem +"_cards").select("user_id", "idcard").distinct()
    val df_comm = df_oper.join(df_card, Seq("user_id"), "left_outer").withColumnRenamed("idcard", "identity_code").persist(StorageLevel.MEMORY_AND_DISK)

    /*// account_info
    spark.read.table("operator_"+ oem +"_v4.account_info").drop("user_mobile").join(df_comm, Seq("identity_code"), "left_outer")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"updated_at".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.account_info")

    // base_info
    spark.read.table("operator_"+ oem +"_v4.base_info").drop("user_mobile").join(df_comm, Seq("identity_code"), "left_outer")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"updated_at".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.base_info")

    // bill_info
    spark.read.table("operator_"+ oem +"_v4.bill_info").drop("user_mobile")
      .groupBy("bill_cycle","bill_discount","bill_fee","bill_total","breach_amount","identity_code","paid_amount","unpaid_amount","usage_detail")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").option("mergeSchema", "false").saveAsTable("operator_"+ oem +"_detail.bill_info")

    // call_info
    spark.read.table("operator_"+ oem +"_v4.call_info").drop("user_mobile")
      .groupBy("call_cycle","identity_code","total_call_count","total_call_time","total_fee")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.call_info")

    // sms_info
    spark.read.table("operator_"+ oem +"_v4.sms_info").drop("user_mobile")
      .groupBy("identity_code","msg_cycle","total_msg_cost","total_msg_count")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.sms_info")*/

    // payment_info
    spark.read.table("operator_"+ oem +"_v4.payment_info").drop("user_mobile")
      .groupBy("identity_code","pay_channel","pay_date","pay_fee","pay_type")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.payment_info")

    /*// bill_record
    spark.read.table("operator_"+ oem +"_v4.bill_record").drop("user_mobile")
      .groupBy("bill_cycle","fee_amount","fee_category","fee_name","identity_code","user_number")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.bill_record")

    // call_record
    spark.read.table("operator_"+ oem +"_v4.call_record").drop("user_mobile")
      .groupBy("call_address","call_cost","call_cycle","call_discount","call_land_type","call_long_distance","call_other_number","call_roam_cost","call_start_time","call_time","call_type_name","identity_code")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(3).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.call_record")

    // sms_record
    spark.read.table("operator_"+ oem +"_v4.sms_record").drop("user_mobile")
      .groupBy("identity_code","msg_address","msg_biz_name","msg_channel","msg_cost","msg_cycle","msg_discount","msg_fee","msg_other_num","msg_remark","msg_start_time","msg_type")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(3).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.sms_record")*/

  }

  def fixDetailAfter(spark: SparkSession, oem: String): Unit ={
    import spark.implicits._
    val df_oper = spark.read.table("buffer.buffer_"+ oem +"_operator").select("user_id", "updated_at").distinct()
    val df_card = spark.read.table("buffer.buffer_"+ oem +"_cards").select("user_id", "idcard").distinct()
    val df_comm = df_oper.join(df_card, Seq("user_id"), "left_outer").withColumnRenamed("idcard", "identity_code").persist(StorageLevel.MEMORY_AND_DISK)

    // account_info
    spark.read.table("operator_"+ oem +"_v4.account_info").drop("user_mobile").withColumnRenamed("operator_auth_date", "updated_at")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"updated_at".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.account_info")

    // base_info
    spark.read.table("operator_"+ oem +"_v4.base_info").drop("user_mobile").withColumnRenamed("operator_auth_date", "updated_at")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"updated_at".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.base_info")

    // bill_info
    spark.read.table("operator_"+ oem +"_v4.bill_info").drop("user_mobile")
      .groupBy("bill_cycle","bill_discount","bill_fee","bill_total","breach_amount","identity_code", "user_id","paid_amount","unpaid_amount","usage_detail")
      .agg(min("occur_time").alias("etl_ms"), min("operator_auth_date").alias("updated_at"))
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.bill_info")

    // call_info
    spark.read.table("operator_"+ oem +"_v4.call_info").drop("user_mobile")
      .groupBy("call_cycle","identity_code", "user_id","total_call_count","total_call_time","total_fee")
      .agg(min("occur_time").alias("etl_ms"), min("operator_auth_date").alias("updated_at"))
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.call_info")

    // sms_info
    spark.read.table("operator_"+ oem +"_v4.sms_info").drop("user_mobile")
      .groupBy("identity_code","user_id","msg_cycle","total_msg_cost","total_msg_count")
      .agg(min("occur_time").alias("etl_ms"), min("operator_auth_date").alias("updated_at"))
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.sms_info")

    // payment_info
    spark.read.table("operator_"+ oem +"_v4.payment_info").drop("user_mobile")
      .groupBy("identity_code","user_id","pay_channel","pay_date","pay_fee","pay_type")
      .agg(min("occur_time").alias("etl_ms"), min("operator_auth_date").alias("updated_at"))
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.payment_info")

    // bill_record
    spark.read.table("operator_"+ oem +"_v4.bill_record").drop("user_mobile")
      .groupBy("bill_cycle","fee_amount","fee_category","fee_name","identity_code","user_number")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.bill_record")

    // call_record
    spark.read.table("operator_"+ oem +"_v4.call_record").drop("user_mobile")
      .groupBy("call_address","call_cost","call_cycle","call_discount","call_land_type","call_long_distance","call_other_number","call_roam_cost","call_start_time","call_time","call_type_name","identity_code")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(3).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.call_record")

    // sms_record
    spark.read.table("operator_"+ oem +"_v4.sms_record").drop("user_mobile")
      .groupBy("identity_code","msg_address","msg_biz_name","msg_channel","msg_cost","msg_cycle","msg_discount","msg_fee","msg_other_num","msg_remark","msg_start_time","msg_type")
      .agg(min("occur_time").alias("etl_ms"))
      .join(df_comm, Seq("identity_code"), "left_outer")
      .repartition(3).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_detail.sms_record")

  }

  def fixReport(spark: SparkSession, oem: String): Unit ={
    import spark.implicits._
    // user_info
    spark.read.table("operator_"+ oem +"_v2.user_info")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.user_info")

    // mobile_info
    spark.read.table("operator_"+ oem +"_v2.mobile_info")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.mobile_info")

    // active_silence_stats
    spark.read.table("operator_"+ oem +"_v2.active_silence_stats")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.active_silence_stats")

    // all_contact_stats
    spark.read.table("operator_"+ oem +"_v2.all_contact_stats")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.all_contact_stats")

    // behavior_analysis
    spark.read.table("operator_"+ oem +"_v2.behavior_analysis")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.behavior_analysis")

    // behavior_score
    spark.read.table("operator_"+ oem +"_v2.behavior_score")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.behavior_score")

    // carrier_consumption_stats
    spark.read.table("operator_"+ oem +"_v2.carrier_consumption_stats")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.carrier_consumption_stats")

    // contact_blacklist_analysis
    spark.read.table("operator_"+ oem +"_v2.contact_blacklist_analysis")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.contact_blacklist_analysis")

    // contact_creditscore_analysis
    spark.read.table("operator_"+ oem +"_v2.contact_creditscore_analysis")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.contact_creditscore_analysis")

    // contact_manyheads_analysis
    spark.read.table("operator_"+ oem +"_v2.contact_manyheads_analysis")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.contact_manyheads_analysis")

    // finance_contact_detail
    spark.read.table("operator_"+ oem +"_v2.finance_contact_detail")
      .groupBy("user_id","identity_code","average_gap_day_call_6month","call_count_1month","call_count_1week","call_count_3month","call_count_6month","call_count_active_3month","call_count_active_6month","call_count_holiday_3month","call_count_holiday_6month","call_count_late_night_3month","call_count_late_night_6month","call_count_offwork_time_3month","call_count_offwork_time_6month","call_count_passive_3month","call_count_passive_6month","call_count_work_time_3month","call_count_work_time_6month","call_count_workday_3month","call_count_workday_6month","call_time_1month","call_time_3month","call_time_6month","call_time_active_3month","call_time_active_6month","call_time_late_night_3month","call_time_late_night_6month","call_time_offwork_time_3month","call_time_offwork_time_6month","call_time_passive_3month","call_time_passive_6month","call_time_work_time_3month","call_time_work_time_6month","contact_area","contact_attribute","contact_name","contact_number","contact_relation","contact_seq_no","contact_type","first_time_call_6month","is_virtual_number","is_whole_day_call_3month","is_whole_day_call_6month","last_time_call_6month","max_gap_day_call_6month","msg_count_1month","msg_count_3month","msg_count_6month")
      .agg(min("operator_auth_date").alias("updated_at"), min("occur_time").alias("etl_ms"))
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.finance_contact_detail")

    // finance_contact_stats
    spark.read.table("operator_"+ oem +"_v2.finance_contact_stats")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.finance_contact_stats")

    // risk_contact_detail
    spark.read.table("operator_"+ oem +"_v2.risk_contact_detail")
      .groupBy("user_id","identity_code","average_gap_day_call_6month","call_count_1month","call_count_1week","call_count_3month","call_count_6month","call_count_active_3month","call_count_active_6month","call_count_holiday_3month","call_count_holiday_6month","call_count_late_night_3month","call_count_late_night_6month","call_count_offwork_time_3month","call_count_offwork_time_6month","call_count_passive_3month","call_count_passive_6month","call_count_work_time_3month","call_count_work_time_6month","call_count_workday_3month","call_count_workday_6month","call_time_1month","call_time_3month","call_time_6month","call_time_active_3month","call_time_active_6month","call_time_late_night_3month","call_time_late_night_6month","call_time_offwork_time_3month","call_time_offwork_time_6month","call_time_passive_3month","call_time_passive_6month","call_time_work_time_3month","call_time_work_time_6month","contact_area","contact_attribute","contact_name","contact_number","contact_relation","contact_seq_no","contact_type","first_time_call_6month","is_virtual_number","is_whole_day_call_3month","is_whole_day_call_6month","last_time_call_6month","max_gap_day_call_6month","msg_count_1month","msg_count_3month","msg_count_6month")
      .agg(min("operator_auth_date").alias("updated_at"), min("occur_time").alias("etl_ms"))
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.risk_contact_detail")

    // risk_contact_stats
    spark.read.table("operator_"+ oem +"_v2.risk_contact_stats")
      .select($"*", row_number().over(Window.partitionBy("identity_code").orderBy($"operator_auth_date".desc)).alias("rn"))
      .where($"rn".<=(1)).drop("rn").withColumnRenamed("operator_auth_date", "updated_at").withColumnRenamed("occur_time", "etl_ms")
      .repartition(1).write.mode("append").partitionBy("etl_ms").format("parquet").saveAsTable("operator_"+ oem +"_report.risk_contact_stats")
  }
}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.mohe.FixOperator \
--master spark://10.1.11.61:7077 \
--executor-memory 20G \
--total-executor-cores 30 \
/opt/taf/jar/lqwork.jar
*/