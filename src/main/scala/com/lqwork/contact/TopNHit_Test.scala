package com.lqwork.contact

import com.lqwork.common.SparkSessionInit
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object TopNHit_Test {
  val spark = SparkSessionInit.initSession("topN_hit")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
//    val oem = args(0)
//    // 所有oem公用的有订单号的订单表
//    val df_order_tmp = getOrder().persist(StorageLevel.MEMORY_AND_DISK)
//    val df_allContacts_tmp = spark.read.table("ods.ods_allContacts").where("oem = '" +oem+ "'").persist(StorageLevel.MEMORY_AND_DISK)


//    callRecordTopN(df_order_tmp, df_allContacts_tmp, oem).withColumn("oem", lit(oem))
//      .coalesce(1).write.mode("append").format("parquet").partitionBy("oem").saveAsTable("ods.topN_contact_stats_tmp")

//    spark.read.table("ods.topN_contact_stats_tmp_zl").coalesce(1)
//      .write.mode("append").format("parquet").partitionBy("oem").saveAsTable("ods.topN_contact_stats_tmp")

    val df_dim = spark.read.table("ods.topN_contact_stats_tmp")/*.where("oem = 'yrd'")*/
      .select($"order_identifier", $"oem",
        $"top20_match_num", $"top20_short_num", $"top20_family_num", (lit(20) - $"top20_family_num").alias("top20_noFamily_num"), $"top20_lessThan11_num",
        $"contacts_num", $"contacts_family_num", ($"contacts_num" - $"contacts_family_num").alias("contacts_noFamily_num"), $"contacts_lessThan11_num", $"6month_call_match_num",
        $"top5_match_num", $"top5_short_num", $"top5_family_num", (lit(5) - $"top5_family_num").alias("top5_noFamily_num"), $"top5_lessThan11_num")

    val df_target = spark.read.table("dwb.dm_risk_stats").where("orderidentifier != ''")/*.where("oem = 'yrd'")*/
      .select($"finishedOrders_num", $"poundage_nums", $"overdue_nums", $"paid_time", $"date", $"deal_nums", $"extend_Down_num", $"old_customer", $"orderidentifier".alias("order_identifier"), $"user_id", $"oem")

    df_target.join(df_dim, Seq("order_identifier", "oem"), "left_outer").na.fill(-1)
      .select(
        $"order_identifier",
        $"oem",
        $"top20_match_num",
        $"top20_short_num",
        $"top20_family_num",
        $"top20_noFamily_num",
        $"top20_lessThan11_num",
        $"contacts_num",
        $"contacts_family_num",
        $"contacts_noFamily_num",
        $"contacts_lessThan11_num",
        $"6month_call_match_num",
        $"top5_match_num",
        $"top5_short_num",
        $"top5_family_num",
        $"top5_noFamily_num",
        $"top5_lessThan11_num",
        $"finishedOrders_num".alias("finished_orders_num"),
        ($"poundage_nums"-$"extend_Down_num").alias("extend_orders"),
        $"overdue_nums".alias("late_orders_num"),
        $"paid_time",
        $"date",
        $"deal_nums".alias("paid_orders_num"),
        lit(0).alias("late_down_num"),
        $"extend_Down_num",
        $"old_customer",
        $"user_id")
      .repartition(1)
      .write.mode("overwrite").format("parquet").saveAsTable("ods.topN_contact_stats")



//    val df_contact = spark.read.table("ods.ods_allContacts").select($"user_id".alias("user_id_c"), $"oem".alias("oem_c")).distinct()
//    val df_target_dim = df_target.join(df_dim, Seq("order_identifier", "oem"), "left_outer")
//    val df_tmp = df_target_dim.join(df_contact, df_target_dim("user_id")===df_contact("user_id_c") && df_target_dim("oem")===df_contact("oem_c"), "left_outer")
//    val df_other = df_tmp.where("user_id_c is not null").na.fill(-1)
//    val df_no_contact = df_tmp.where("user_id_c is null").na.fill(-2)
//    df_other.union(df_no_contact)
//      .repartition(1)
//      .write.mode("overwrite").format("parquet").saveAsTable("ods.topN_contact_stats")


//    df_target.join(df_dim, Seq("order_identifier", "oem"), "left_outer")
//      .coalesce(1).write.mode("overwrite").format("parquet").saveAsTable("ods.topN_contact_stats")

    spark.close()
  }

  /**
    * 通话记录top20统计、通讯录统计信息、通话记录top5统计聚合汇总
    */
  private def callRecordTopN(df_order_tmp: Dataset[Row], df_allContacts_tmp: Dataset[Row], oem: String): Dataset[Row] = {
    // 身份证表
    val df_idCard = spark.read.table("buffer.buffer_" +oem+ "_cards").select("user_id", "idcard").persist(StorageLevel.MEMORY_AND_DISK)
    // 母订单表
    val df_order = df_order_tmp.where("oem = '" + oem +"'")
    // 母订单表关联用户身份证号
    val df_order_idcard = df_order.join(df_idCard, Seq("user_id"), "left_outer").select("user_id", "idcard", "order_identifier", "currTime", "sixMonthAgo")
    // 通话记录明细
    val df_call = spark.read.table("operator_" +oem+ "_v4.call_record")
      .select($"identity_code"/*.alias("idcard")*/, $"call_other_number", $"call_start_time")
      .distinct()
    // 近6个月通话记录电话号
    val df_call_phones = df_order_idcard.join(df_call,
      df_order_idcard("idcard") === df_call("identity_code") &&
      df_call("call_start_time") > df_order_idcard("sixMonthAgo") &&
      df_call("call_start_time") < df_order_idcard("currTime"),
      "left_outer")
      .select($"user_id", $"order_identifier", $"call_other_number".alias("tel"))
      .groupBy("user_id", "order_identifier", "tel").agg(count("tel").alias("cnt"))
      .persist(StorageLevel.MEMORY_AND_DISK)
    // 用户通讯录联系人
    val df_allContacts = df_allContacts_tmp.where("oem = '" + oem +"'")
      .select($"user_id", $"contacts_name", $"contacts_phone".alias("tel"))/*.distinct()*/
      .persist(StorageLevel.MEMORY_AND_DISK)
    // 通讯录中是家人的联系人
    val df_family = df_allContacts.where(
        "contacts_name like '%爸%' or contacts_name like '%妈%' or " +
        "contacts_name like '%父%' or contacts_name like '%母%' or " +
        "contacts_name like '%儿子%' or contacts_name like '%女儿%' ")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val df_stats = top20Stats(df_call_phones,df_allContacts,df_family)
      .join(contactStats(df_order,df_call_phones,df_allContacts,df_family), Seq("user_id", "order_identifier"), "left_outer")
      .join(top5Stats(df_call_phones,df_allContacts,df_family), Seq("user_id", "order_identifier"), "left_outer")
      .join(df_idCard, Seq("user_id"), "left_outer")

    // 运营商数据中的基本信息表, 每个用户对应一条表中数据
    val df_oper = spark.read.table("operator_" +oem+ "_v4.base_info").select($"identity_code".alias("idcard_oper")).distinct()
    val tmp = df_stats.join(df_oper, df_stats("idcard") === df_oper("idcard_oper"), "left_outer").persist(StorageLevel.MEMORY_AND_DISK)
    // 有运营商数据
    val df_have = tmp.where("idcard_oper is not null").na.fill(0)
    // 无运营商数据
    val df_no = tmp.where("idcard_oper is null")
      .select($"user_id",$"order_identifier",
        lit(null).alias("top20_match_num"),lit(null).alias("top20_short_num"),lit(null).alias("top20_lessThan11_num"),lit(null).alias("top20_family_num"),
        $"contacts_num",$"contacts_family_num",$"contacts_lessThan11_num",$"6month_call_match_num",
        lit(null).alias("top5_match_num"),lit(null).alias("top5_short_num"),lit(null).alias("top5_lessThan11_num"),lit(null).alias("top5_family_num"),
        $"idcard", $"idcard_oper")

//    println(df_call_phones.select("user_id").distinct().count())
//    println(df_stats.count())
//    println(df_oper.count())
//    println(tmp.count())
//    println(df_have.count())

    df_have.union(df_no)

  }

  /**
    * 通讯录统计信息
    */
  private def contactStats(df_order: Dataset[Row], df_call_phones: Dataset[Row], df_allContacts: Dataset[Row], df_family: Dataset[Row]): Dataset[Row] ={
    import spark.implicits._

    // 通讯录人数
    val df_contacts = df_order.join(df_allContacts.groupBy("user_id").agg(count("tel").alias("contacts_num")), Seq("user_id"))
      .select("user_id", "order_identifier", "contacts_num")
    // 近6个月通讯录与运营商通话记录中的匹配数量
    val df_6month_call_match = df_call_phones.join(df_allContacts, Seq("user_id", "tel"))
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("6month_call_match_num"))
      .select("user_id", "order_identifier", "6month_call_match_num")
    // 通讯录中有父母子女
    val df_contactsFamily = df_order.join(df_family.groupBy("user_id").agg(count("tel").alias("contacts_family_num")), Seq("user_id"))
      .select("user_id", "order_identifier", "contacts_family_num")
    // 通讯录中少于11位正常号码数
    val df_contactsLessThan11 = df_order.join(df_allContacts.where(length($"tel") < 11)
      .groupBy("user_id").agg(count("tel").alias("contacts_lessThan11_num")), Seq("user_id"))
      .select("user_id", "order_identifier", "contacts_lessThan11_num")

    df_contacts
      .join(df_contactsFamily, Seq("user_id", "order_identifier"), "left_outer")
      .join(df_contactsLessThan11, Seq("user_id", "order_identifier"), "left_outer")
      .join(df_6month_call_match, Seq("user_id", "order_identifier"), "left_outer")
  }

  /**
    * 通话记录top20统计
    */
  private def top20Stats(df_call_phones: Dataset[Row], df_allContacts: Dataset[Row], df_family: Dataset[Row]): Dataset[Row] ={
    import spark.implicits._

    // 通话记录中订单申请前6个月最常联系的20个人
    val df_callTop20 = df_call_phones
      .select($"user_id", $"order_identifier", $"tel", row_number().over(Window.partitionBy("order_identifier").orderBy($"cnt".desc)).alias("rn"))
      .where($"rn".<=(20))
      .select("user_id", "order_identifier", "tel")
      .persist(StorageLevel.MEMORY_AND_DISK)
    // 通话记录最常联系前20在本人通讯录中的个数
    val df_top20match = df_callTop20.join(df_allContacts, Seq("user_id", "tel"))
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top20_match_num"))
      .select("user_id", "order_identifier", "top20_match_num")
    // 最常联系前20短号数
    val df_top20ShortPhone = df_callTop20.where($"tel".like("6%") && length($"tel") >= 3 && length($"tel") <= 6)
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top20_short_num"))
      .select("user_id", "order_identifier", "top20_short_num")

    // 最常联系前20少于11位的号码数
    val df_top20LessThan11 = df_callTop20.where(length($"tel") < 11)
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top20_lessThan11_num"))
      .select("user_id", "order_identifier", "top20_lessThan11_num")

    //最常联系前20中有父母子女数
    val df_top20Family = df_callTop20.join(df_family, Seq("user_id", "tel"))
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top20_family_num"))
      .select("user_id", "order_identifier", "top20_family_num")

    df_top20match
      .join(df_top20ShortPhone, Seq("user_id", "order_identifier"), "left_outer")
      .join(df_top20LessThan11, Seq("user_id", "order_identifier"), "left_outer")
      .join(df_top20Family, Seq("user_id", "order_identifier"), "left_outer")
  }

  /**
    * 通话记录top5统计
    */
  private def top5Stats(df_call_phones: Dataset[Row], df_allContacts: Dataset[Row], df_family: Dataset[Row]): Dataset[Row] ={
    import spark.implicits._

    // 通话记录中订单申请前6个月最常联系的5个人
    val df_callTop5 = df_call_phones
      .select($"user_id", $"order_identifier", $"tel", row_number().over(Window.partitionBy("order_identifier").orderBy($"cnt".desc)).alias("rn"))
      .where($"rn".<=(5))
      .select("user_id", "order_identifier", "tel")
      .persist(StorageLevel.MEMORY_AND_DISK)
    // 通话记录最常联系前5在本人通讯录中的个数
    val df_top5match = df_callTop5.join(df_allContacts, Seq("user_id", "tel"))
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top5_match_num"))
      .select("user_id", "order_identifier", "top5_match_num")
    // 最常联系前5短号数
    val df_top5ShortPhone = df_callTop5.where($"tel".like("6%") && length($"tel") >= 3 && length($"tel") <= 6)
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top5_short_num"))
      .select("user_id", "order_identifier", "top5_short_num")
    // 最常联系前5少于11位的号码数
    val df_top5LessThan11 = df_callTop5.where(length($"tel") < 11)
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top5_lessThan11_num"))
      .select("user_id", "order_identifier", "top5_lessThan11_num")
    // 最常联系前5中有父母子女数
    val df_top5Family = df_callTop5.join(df_family, Seq("user_id", "tel"))
      .groupBy("user_id", "order_identifier").agg(count("tel").alias("top5_family_num"))
      .select("user_id", "order_identifier", "top5_family_num")

    df_top5match
      .join(df_top5ShortPhone, Seq("user_id", "order_identifier"), "left_outer")
      .join(df_top5LessThan11, Seq("user_id", "order_identifier"), "left_outer")
      .join(df_top5Family, Seq("user_id", "order_identifier"), "left_outer")
  }

  /**
    * 母订单表
    */
  private def getOrder(): Dataset[Row] ={
    spark.read.table("ods.ods_order_checked").select($"user_id", $"order_identifier", $"apply_time".alias("currTime"),
      concat_ws(" ", date_sub($"apply_time", 180), substring($"apply_time", 12, 10)).alias("sixMonthAgo"))
  }

}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.contact.TopNHit_Test \
--master spark://10.1.11.61:7077 \
--executor-memory 30G \
--total-executor-cores 20 \
/opt/taf/jar/lqwork.jar
*/