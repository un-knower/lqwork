package com.lqwork.mohe.old

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json.{JSONArray, JSONObject}

object WriteIntoHive {

  /*
  保存用户基本信息、运营商消费记录、通话记录、短信记录等到Hive表
   */
  def writeOperToHive(spark:SparkSession, occur_time:String, oem: String, dataRDD: RDD[(JSONObject,JSONObject,JSONArray,JSONArray,JSONArray,JSONArray)]): Unit ={
    val base_info = dataRDD.map(_._1).filter(_.length() != 0).map(_.toString)
    spark.read.json(base_info).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.base_info")

    val account_info = dataRDD.map(_._2).filter(_.length() != 0).map(_.toString)
    spark.read.json(account_info).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.account_info")

    val payment_info = dataRDD.map(_._4).filter(_.length() != 0).map(_.toString)
    spark.read.json(payment_info).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.payment_info")

    val bill_info = dataRDD.map(_._3).filter(_.length() != 0).map(_.toString)
    spark.read.json(bill_info).drop("bill_record").withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.bill_info")

    val call_info = dataRDD.map(_._5).filter(_.length() != 0).map(_.toString)
    spark.read.json(call_info).drop("call_record").withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.call_info")

    val sms_info = dataRDD.map(_._6).filter(_.length() != 0).map(_.toString)
    spark.read.json(sms_info).drop("sms_record").withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.sms_info")

    spark.read.table("operator_"+ oem +"_v4.call_record").select("user_id", "operator_auth_date")

    val bill_record = dataRDD.map(data => JsonParse.getRecord(data._3, "bill_record", "bill_cycle")).filter(_.length() != 0).map(_.toString)
    spark.read.json(bill_record).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.bill_record")

    val call_record = dataRDD.map(data => JsonParse.getRecord(data._5, "call_record", "call_cycle")).filter(_.length() != 0).map(_.toString)
    spark.read.json(call_record).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.call_record")

    val sms_record = dataRDD.map(data => JsonParse.getRecord(data._6, "sms_record", "msg_cycle")).filter(_.length() != 0).map(_.toString)
    spark.read.json(sms_record).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v4.sms_record")
  }

  /*
  保存用户基本信息、金融联系人统计信息、风险联系人统计信息等到Hive表
   */
  def writeFinToHive(spark: SparkSession, occur_time: String, oem: String,  dataRDD: RDD[(String, String, String, String,String, String, String, String,String, String, String, String,String, String)]): Unit ={
    val user_info = dataRDD.map(_._1).filter(!_.equals("empty"))
    spark.read.json(user_info).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.user_info")

    val mobile_info = dataRDD.map(_._2).filter(!_.equals("empty"))
    spark.read.json(mobile_info).drop("identity_code").withColumnRenamed("idCard","identity_code").withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.mobile_info")

    val risk_contact_stats = dataRDD.map(_._3).filter(!_.equals("empty"))
    spark.read.json(risk_contact_stats).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.risk_contact_stats")

    val risk_contact_detail = dataRDD.map(_._4).filter(!_.equals("empty"))
    spark.read.json(risk_contact_detail).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.risk_contact_detail")

    val finance_contact_stats = dataRDD.map(_._5).filter(!_.equals("empty"))
    spark.read.json(finance_contact_stats).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.finance_contact_stats")

    val finance_contact_detail = dataRDD.map(_._6).filter(!_.equals("empty"))
    spark.read.json(finance_contact_detail).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.finance_contact_detail")

    val behavior_score = dataRDD.map(_._7).filter(!_.equals("empty"))
    spark.read.json(behavior_score).select("user_id","identity_code","base_info_score","bill_info_score","call_info_score","risk_contact_info_score","total_score","operator_auth_date")
      .withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.behavior_score")

    val contact_blacklist_analysis = dataRDD.map(_._8).filter(!_.equals("empty"))
    spark.read.json(contact_blacklist_analysis).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.contact_blacklist_analysis")

    val contact_manyheads_analysis = dataRDD.map(_._9).filter(!_.equals("empty"))
    spark.read.json(contact_manyheads_analysis).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.contact_manyheads_analysis")

    val contact_creditscore_analysis = dataRDD.map(_._10).filter(!_.equals("empty"))
    spark.read.json(contact_creditscore_analysis).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_"+ oem +"_v2.contact_creditscore_analysis")

    val all_contact_stats = dataRDD.map(_._11).filter(!_.equals("empty"))
    val carrier_consumption_stats = dataRDD.map(_._12).filter(!_.equals("empty"))
    if(Array("aj","ddh","ddhs","hjsj","jm","yhd").contains(oem)){
      spark.read.json(all_contact_stats).withColumn("occur_time", lit(occur_time))
        .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_" + oem + "_v2.all_contact_stats")

      spark.read.json(carrier_consumption_stats).withColumn("occur_time", lit(occur_time))
        .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_" + oem + "_v2.carrier_consumption_stats")
    } else {
      spark.read.json(all_contact_stats)
        .select("call_count_1month","call_count_3month","call_count_6month","call_count_active_3month","call_count_active_6month","call_count_active_late_night_6month","call_count_call_time_1min5min_6month","call_count_call_time_5min10min_6month","call_count_call_time_less1min_6month","call_count_call_time_over10min_6month","call_count_holiday_3month","call_count_holiday_6month","call_count_late_night_3month","call_count_late_night_6month","call_count_offwork_time_3month","call_count_offwork_time_6month","call_count_passive_3month","call_count_passive_6month","call_count_passive_late_night_6month","call_count_work_time_3month","call_count_work_time_6month","call_count_workday_3month","call_count_workday_6month","call_time_1month","call_time_3month","call_time_6month","call_time_active_3month","call_time_active_6month","call_time_active_mobile_6month","call_time_late_night_3month","call_time_late_night_6month","call_time_offwork_time_3month","call_time_offwork_time_6month","call_time_passive_3month","call_time_passive_6month","call_time_passive_mobile_6month","call_time_work_time_3month","call_time_work_time_6month","contact_count_1month","contact_count_3month","contact_count_6month","contact_count_active_3month","contact_count_active_6month","contact_count_call_count_over10_3month","contact_count_call_count_over10_6month","contact_count_mobile_6month","contact_count_mutual_3month","contact_count_mutual_6month","contact_count_not_mobile_telephone_6month","contact_count_passive_3month","contact_count_passive_6month","contact_count_telephone_6month","identity_code","msg_count_1month","msg_count_3month","msg_count_6month", "user_id", "operator_auth_date")
        .withColumn("occur_time", lit(occur_time))
        .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_" + oem + "_v2.all_contact_stats")

      spark.read.json(carrier_consumption_stats)
        .select("consume_amount_1month","consume_amount_3month","consume_amount_6month","identity_code","recharge_amount_1month","recharge_amount_3month","recharge_amount_6month","recharge_count_1month","recharge_count_3month","recharge_count_6month", "user_id", "operator_auth_date")
        .withColumn("occur_time", lit(occur_time))
        .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_" + oem + "_v2.carrier_consumption_stats")
    }

    val active_silence_stats = dataRDD.map(_._13).filter(!_.equals("empty"))
    spark.read.json(active_silence_stats)
      .select("active_day_1call_3month","active_day_1call_6month","continue_silence_day_over15_0call_0msg_send_3month","continue_silence_day_over15_0call_0msg_send_6month","continue_silence_day_over15_0call_3month","continue_silence_day_over15_0call_6month","continue_silence_day_over15_0call_active_3month","continue_silence_day_over15_0call_active_6month","continue_silence_day_over3_0call_0msg_send_3month","continue_silence_day_over3_0call_0msg_send_6month","continue_silence_day_over3_0call_3month","continue_silence_day_over3_0call_6month","continue_silence_day_over3_0call_active_3month","continue_silence_day_over3_0call_active_6month","gap_day_last_silence_day_0call_0msg_send_6month","gap_day_last_silence_day_0call_6month","gap_day_last_silence_day_0call_active_6month","identity_code","max_continue_active_day_1call_3month","max_continue_active_day_1call_6month","max_continue_silence_day_0call_0msg_send_3month","max_continue_silence_day_0call_0msg_send_6month","max_continue_silence_day_0call_3month","max_continue_silence_day_0call_6month","max_continue_silence_day_0call_active_3month","max_continue_silence_day_0call_active_6month","silence_day_0call_0msg_send_3month","silence_day_0call_0msg_send_6month","silence_day_0call_3month","silence_day_0call_6month","silence_day_0call_active_3month","silence_day_0call_active_6month", "user_id", "operator_auth_date")
      .withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_" + oem + "_v2.active_silence_stats")

    val behavior_analysis = dataRDD.map(_._14).filter(!_.equals("empty"))
    spark.read.json(behavior_analysis).withColumn("occur_time", lit(occur_time))
      .coalesce(1).write.mode("append").partitionBy("occur_time").format("parquet").saveAsTable("operator_" + oem + "_v2.behavior_analysis")
  }
}
