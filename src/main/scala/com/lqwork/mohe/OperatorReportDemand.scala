package com.lqwork.mohe

import java.time.LocalDate

import com.lqwork.common.SparkSessionInit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object OperatorReportDemand {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionInit.initSession("operator_report_new")
    val occur_time = LocalDate.now().toString
    // 所有oem公用的有订单号的订单表
    val df_order_tmp = spark.read.table("ods.ods_risk_orderUsers01").where("order_identifier is not null").persist(StorageLevel.MEMORY_AND_DISK)

    /*// 停止放贷的oem
    getFields(spark, df_order_tmp, "baobao")
      .union(getFields(spark, df_order_tmp, "jybk"))
      .union(getFields(spark, df_order_tmp, "xzbk"))
      .union(getFields(spark, df_order_tmp, "yrd"))
      .union(getFields(spark, df_order_tmp, "msdq"))
      .union(getFields(spark, df_order_tmp, "ddhs"))
      .union(getFields(spark, df_order_tmp, "hjsj"))
      .union(getFields(spark, df_order_tmp, "jm"))
      .union(getFields(spark, df_order_tmp, "yhd"))
      .coalesce(1).write.mode("overwrite").format("parquet").saveAsTable("ods.ods_operator_report_stop")*/

    val df_operator_report_stop = spark.read.table("ods.ods_operator_report_stop")
    df_operator_report_stop
      .union(getFields(spark, df_order_tmp, "xzbk2"))
      .union(getFields(spark, df_order_tmp, "aj"))
      .union(getFields(spark, df_order_tmp, "ddh"))
      .union(getFields(spark, df_order_tmp, "hjh"))
      .union(getFields(spark, df_order_tmp, "ymbk"))
      .union(getFields(spark, df_order_tmp, "sdhh"))
      .union(getFields(spark, df_order_tmp, "kb"))
      .union(getFields(spark, df_order_tmp, "bb"))
      .union(getFields(spark, df_order_tmp, "lxb"))
      .union(getFields(spark, df_order_tmp, "lxbh5"))
      .union(getFields(spark, df_order_tmp, "lxb2"))
      .union(getFields(spark, df_order_tmp, "sdhhh5"))
      .withColumn("occur_time", lit(occur_time)).withColumn("plat", lit("pgd"))
      .coalesce(1).write.mode("overwrite").format("parquet").saveAsTable("ods.ods_finance_second_new")

    spark.close()
  }

  def getFields(spark: SparkSession, df_order_tmp: DataFrame, oem: String): Dataset[Row] ={
    import spark.implicits._

    val df_mi = spark.read.table("operator_"+ oem +"_v2.mobile_info")
      .select($"identity_code", $"operator_auth_date", $"user_id", $"account_status", $"account_balance", $"mobile_carrier")
      .groupBy($"identity_code", $"operator_auth_date", $"user_id")
      .agg(max("account_status") as "account_status", max("account_balance") as "account_balance", max("mobile_carrier") as "mobile_carrier")

    val df_bs = spark.read.table("operator_"+ oem +"_v2.behavior_score")
      .select($"identity_code", $"operator_auth_date", $"call_info_score", $"base_info_score", $"risk_contact_info_score", $"bill_info_score", $"total_score")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max("call_info_score") as "call_info_score",max("base_info_score") as "base_info_score",max("risk_contact_info_score") as "risk_contact_info_score",
        max("bill_info_score") as "bill_info_score",max("total_score") as "total_score")

    val df_cba = spark.read.table("operator_"+ oem +"_v2.contact_blacklist_analysis")
      .select($"identity_code", $"operator_auth_date", $"black_top10_contact_total_count_ratio", $"black_top10_contact_creditcrack_count_ratio")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max("black_top10_contact_total_count_ratio") as "black_top10_contact_total_count_ratio",
        max("black_top10_contact_creditcrack_count_ratio") as "black_top10_contact_creditcrack_count_ratio")

    val df_cma = spark.read.table("operator_"+ oem +"_v2.contact_manyheads_analysis")
      .select($"identity_code", $"operator_auth_date", $"manyheads_top10_contact_recent6month_partnercode_count_max", $"manyheads_top10_contact_recent3month_partnercode_count_max")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max("manyheads_top10_contact_recent6month_partnercode_count_max") as "manyheads_top10_contact_recent6month_partnercode_count_max",
        max("manyheads_top10_contact_recent3month_partnercode_count_max") as "manyheads_top10_contact_recent3month_partnercode_count_max")

    val df_cca = spark.read.table("operator_"+ oem +"_v2.contact_creditscore_analysis")
      .select($"identity_code", $"operator_auth_date", $"creditscore_top10_contact_avg")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max("creditscore_top10_contact_avg") as "creditscore_top10_contact_avg")

    val df_acs = spark.read.table("operator_"+ oem +"_v2.all_contact_stats")
      .select($"identity_code", $"operator_auth_date", $"contact_count_3month", $"contact_count_mutual_3month", $"call_count_3month", $"call_count_passive_3month", $"call_count_late_night_3month",
      $"call_time_3month", $"msg_count_1month", $"contact_count_1month", $"call_time_1month", $"call_count_active_3month")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max("contact_count_3month") as "contact_count_3month", max("contact_count_mutual_3month") as "contact_count_mutual_3month",
        max("call_count_3month") as "call_count_3month", max("call_count_passive_3month") as "call_count_passive_3month", max("call_count_late_night_3month") as "call_count_late_night_3month",
        max("call_time_3month") as "call_time_3month", max("msg_count_1month") as "msg_count_1month", max("contact_count_1month") as "contact_count_1month",
        max("call_time_1month") as "call_time_1month", max("call_count_active_3month") as "call_count_active_3month")

    val df_ccs = spark.read.table("operator_"+ oem +"_v2.carrier_consumption_stats")
      .select($"identity_code", $"operator_auth_date", $"consume_amount_3month", $"recharge_count_3month", $"recharge_amount_3month", $"recharge_amount_1month", $"consume_amount_1month")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max("consume_amount_3month") as "consume_amount_3month", max("recharge_count_3month") as "recharge_count_3month",
        max("recharge_amount_3month") as "recharge_amount_3month", max("recharge_amount_1month") as "recharge_amount_1month",
        max("consume_amount_1month") as "consume_amount_1month")

    val df_ass = spark.read.table("operator_"+ oem +"_v2.active_silence_stats")
      .select($"identity_code", $"operator_auth_date", $"active_day_1call_3month", $"silence_day_0call_3month", $"max_continue_silence_day_0call_3month")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max($"active_day_1call_3month") as "active_day_1call_3month",
        max($"silence_day_0call_3month") as "silence_day_0call_3month",
        max($"max_continue_silence_day_0call_3month") as "max_continue_silence_day_0call_3month")

    val df_ba = spark.read.table("operator_"+ oem +"_v2.behavior_analysis")
      .select($"identity_code", $"operator_auth_date", $"mobile_silence_analysis_6month")
      .groupBy($"identity_code", $"operator_auth_date")
      .agg(max("mobile_silence_analysis_6month") as "mobile_silence_analysis_6month")

    //金融风险信息
    val df_fin = df_mi.join(df_bs, Seq("identity_code", "operator_auth_date"), "left_outer")
      .join(df_cba, Seq("identity_code", "operator_auth_date"), "left_outer")
      .join(df_cma, Seq("identity_code", "operator_auth_date"), "left_outer")
      .join(df_cca, Seq("identity_code", "operator_auth_date"), "left_outer")
      .join(df_acs, Seq("identity_code", "operator_auth_date"), "left_outer")
      .join(df_ccs, Seq("identity_code", "operator_auth_date"), "left_outer")
      .join(df_ass, Seq("identity_code", "operator_auth_date"), "left_outer")
      .join(df_ba,  Seq("identity_code", "operator_auth_date"), "left_outer")
      .withColumn("oem", lit(oem)).where("operator_auth_date is not null")
      .withColumn("operator_auth_date", substring($"operator_auth_date", 0, 10))
      .withColumnRenamed("user_id", "user_id_fin")
      .withColumnRenamed("oem", "fin_oem").distinct()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    //运营商入网时间
    val df_net_time = spark.read.table("operator_"+ oem +"_v4.account_info")
      .select($"identity_code", $"net_time")
      .groupBy("identity_code").agg("net_time" -> "min")
      .withColumnRenamed("min(net_time)", "net_time")





    //订单相关字段
    val df_order = df_order_tmp.where("oem = '"+ oem + "'")
      .select($"identity_code", $"user_id", $"order_identifier", $"apply_time", $"oem")
      .join(df_net_time, Seq("identity_code"), "left_outer")
      .select($"user_id", $"order_identifier", $"apply_time", $"net_time", $"oem")

    //订单关联金融风险联系人
    val df = df_order.join(
      df_fin,
      df_order("user_id") === df_fin("user_id_fin") && df_order("apply_time").substr(0, 7) === df_fin("operator_auth_date").substr(0, 7),
      "left_outer").persist(StorageLevel.MEMORY_AND_DISK)

    //最新一笔订单跟上一笔订单之间超过一个月
    val df_1 = df.where("operator_auth_date is not null")

    //最新一笔订单跟上一笔订单之间不超过一个月
    val df_order_null = df.where("operator_auth_date is null").select($"user_id", $"order_identifier", $"apply_time", $"net_time", $"oem")

    val df_2 = df_order_null.join(df_fin,
      df_order_null("user_id") === df_fin("user_id_fin")
        && date_sub(concat(substring(df_order_null("apply_time"), 0, 8), lit("15 00:00:00")), 30).substr(0, 7) === df_fin("operator_auth_date").substr(0, 7),
      "inner"
    )

    val df_2_orderid = df_2.select($"order_identifier").withColumnRenamed("order_identifier", "order_identifier_")
    val df_3 = df_order_null.join(df_2_orderid, df_order_null("order_identifier") === df_2_orderid("order_identifier_"), "left_outer")
      .where("order_identifier_ is null").drop("order_identifier_")
    val df_3_fin = df_3.join(df_fin, df_3("user_id") === df_fin("user_id_fin"), "left_outer").drop("identity_code", "user_id_fin", "fin_oem")

    df_1.union(df_2).drop("identity_code", "user_id_fin", "fin_oem").union(df_3_fin).createOrReplaceTempView("temp")
    spark.sql("select user_id, order_identifier, oem, max(apply_time) apply_time, max(operator_auth_date) operator_auth_date, max(account_status) account_status, max(account_balance) account_balance, max(net_time) mobile_net_time, max(mobile_carrier) mobile_carrier, max(call_info_score) call_info_score, max(black_top10_contact_total_count_ratio) black_top10_contact_total_count_ratio, max(black_top10_contact_creditcrack_count_ratio) black_top10_contact_creditcrack_count_ratio, max(manyheads_top10_contact_recent6month_partnercode_count_max) manyheads_top10_contact_recent6month_partnercode_count_max,  max(manyheads_top10_contact_recent3month_partnercode_count_max) manyheads_top10_contact_recent3month_partnercode_count_max, max(creditscore_top10_contact_avg) creditscore_top10_contact_avg, max(contact_count_3month) contact_count_3month, max(contact_count_mutual_3month) contact_count_mutual_3month, max(call_count_3month) call_count_3month, max(call_count_passive_3month) call_count_passive_3month, max(call_count_late_night_3month) call_count_late_night_3month, max(consume_amount_3month) consume_amount_3month, max(recharge_count_3month) recharge_count_3month, max(recharge_amount_3month) recharge_amount_3month, max(active_day_1call_3month) active_day_1call_3month, max(silence_day_0call_3month) silence_day_0call_3month, max(max_continue_silence_day_0call_3month) max_continue_silence_day_0call_3month, max(mobile_silence_analysis_6month) mobile_silence_analysis_6month, max(call_time_3month) call_time_3month, max(msg_count_1month) msg_count_1month, max(contact_count_1month) contact_count_1month, max(call_time_1month) call_time_1month, max(call_count_active_3month) call_count_active_3month,max(recharge_amount_1month) recharge_amount_1month, max(consume_amount_1month) consume_amount_1month,max(base_info_score) base_info_score,max(risk_contact_info_score) risk_contact_info_score,max(bill_info_score) bill_info_score,max(total_score) total_score " +
      "from temp group by oem, user_id, order_identifier ")

    /*//订单相关字段
    val df_order = spark.read.table("ods.ods_risk_orderUsers")
      .where("oem = '"+ oem + "'")
      .select($"identity_code", $"user_id", $"order_identifier", $"real_lead_date", $"oem")
      .join(df_net_time, Seq("identity_code"), "left_outer")
      .select($"user_id", $"order_identifier", $"real_lead_date", $"net_time", $"oem")

    //订单关联金融风险联系人
    val df = df_order.join(df_fin,
      df_order("user_id") === df_fin("user_id_fin")
      && df_order("real_lead_date").substr(0, 7) === df_fin("operator_auth_date").substr(0, 7),
      "left_outer"
    ).persist(StorageLevel.MEMORY_AND_DISK)

    //最新一笔订单跟上一笔订单之间超过一个月
    val df_1 = df.where("operator_auth_date is not null")

    //最新一笔订单跟上一笔订单之间不超过一个月
    val df_order_null = df.where("operator_auth_date is null").select($"user_id", $"order_identifier", $"real_lead_date", $"net_time", $"oem")

    val df_2 = df_order_null.join(df_fin,
      df_order_null("user_id") === df_fin("user_id_fin")
      && date_sub(concat(substring(df_order_null("real_lead_date"), 0, 8), lit("15 00:00:00")), 30).substr(0, 7) === df_fin("operator_auth_date").substr(0, 7),
      "inner"
    )

    val df_2_orderid = df_2.select($"order_identifier").withColumnRenamed("order_identifier", "order_identifier_")
    val df_3 = df_order_null.join(df_2_orderid, df_order_null("order_identifier") === df_2_orderid("order_identifier_"), "left_outer")
      .where("order_identifier_ is null").drop("order_identifier_")
    val df_3_fin = df_3.join(df_fin, df_3("user_id") === df_fin("user_id_fin"), "left_outer").drop("identity_code", "user_id_fin", "fin_oem")

    df_1.union(df_2).drop("identity_code", "user_id_fin", "fin_oem").union(df_3_fin).createOrReplaceTempView("temp")
    spark.sql("select user_id, order_identifier, oem, max(real_lead_date) real_lead_date, max(operator_auth_date) operator_auth_date, max(account_status) account_status, max(account_balance) account_balance, max(net_time) mobile_net_time, max(mobile_carrier) mobile_carrier, max(call_info_score) call_info_score, max(black_top10_contact_total_count_ratio) black_top10_contact_total_count_ratio, max(black_top10_contact_creditcrack_count_ratio) black_top10_contact_creditcrack_count_ratio, max(manyheads_top10_contact_recent6month_partnercode_count_max) manyheads_top10_contact_recent6month_partnercode_count_max,  max(manyheads_top10_contact_recent3month_partnercode_count_max) manyheads_top10_contact_recent3month_partnercode_count_max, max(creditscore_top10_contact_avg) creditscore_top10_contact_avg, max(contact_count_3month) contact_count_3month, max(contact_count_mutual_3month) contact_count_mutual_3month, max(call_count_3month) call_count_3month, max(call_count_passive_3month) call_count_passive_3month, max(call_count_late_night_3month) call_count_late_night_3month, max(consume_amount_3month) consume_amount_3month, max(recharge_count_3month) recharge_count_3month, max(recharge_amount_3month) recharge_amount_3month, max(active_day_1call_3month) active_day_1call_3month, max(silence_day_0call_3month) silence_day_0call_3month, max(max_continue_silence_day_0call_3month) max_continue_silence_day_0call_3month, max(mobile_silence_analysis_6month) mobile_silence_analysis_6month, max(call_time_3month) call_time_3month, max(msg_count_1month) msg_count_1month, max(contact_count_1month) contact_count_1month, max(call_time_1month) call_time_1month, max(call_count_active_3month) call_count_active_3month,max(recharge_amount_1month) recharge_amount_1month, max(consume_amount_1month) consume_amount_1month,max(base_info_score) base_info_score,max(risk_contact_info_score) risk_contact_info_score,max(bill_info_score) bill_info_score,max(total_score) total_score " +
      "from temp group by user_id, order_identifier, oem")*/

  }
}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.mohe.OperatorReportDemand \
--master spark://10.1.11.61:7077 \
--executor-memory 10G \
--total-executor-cores 20 \
/opt/taf/jar/lqwork.jar
*/