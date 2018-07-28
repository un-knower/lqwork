package com.lqwork.mohe.old

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.json.{JSONArray, JSONObject}

object JsonParse {

  def getOperateJson(spark: SparkSession, operHDFSPath: String, occur_time: String): RDD[(JSONObject, JSONObject, JSONArray, JSONArray, JSONArray, JSONArray)] ={
    val dataRDD = spark.sparkContext.textFile(operHDFSPath + occur_time).coalesce(100).map(line => {
      val data = line.split("\\^", -1)
      try {
        val json = new JSONObject(data(1))
        val base_info    = getSimpleInfo("base_info", json, data(0), data(2))
        val account_info = getSimpleInfo("account_info", json, data(0), data(2))
        val bill_info    = getInfo("bill_info", json, data(0), data(2))
        val payment_info = getInfo("payment_info", json, data(0), data(2))
        val call_info    = getInfo("call_info", json, data(0), data(2))
        val sms_info     = getInfo("sms_info", json, data(0), data(2))

        Tuple6(base_info, account_info, bill_info, payment_info, call_info, sms_info)
      } catch {
        case ex: Exception => println("user_id----->" + data(0) + "\n" + ex)
          Tuple6(new JSONObject(), new JSONObject(), new JSONArray(), new JSONArray(), new JSONArray(), new JSONArray())
      }
    })

    dataRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def getFinanceJson(spark: SparkSession, finHDFSPath: String, occur_time: String): RDD[(String, String, String, String,String, String, String, String,String, String, String, String,String, String)] ={
    val dataRDD = spark.sparkContext.textFile(finHDFSPath + occur_time).coalesce(100).map(line => {
      val data = line.split("\\^", -1)
      if(StringUtils.isNotBlank(data(1)) && !"null".equalsIgnoreCase(data(1))){
        try {
          val json = new JSONObject(data(1))
          // user_info
          val ui = json.getJSONObject("user_info")
          val uiStr  = ui.put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          // mobile_info
          val identity_code = ui.getString("identity_code")
          val miStr = json.getJSONObject("mobile_info").put("idCard",identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //risk_contact_stats
          val risk_arr = json.getJSONArray("risk_contact_stats")
          for(i <- 0 until risk_arr.length()){
            val risk = risk_arr.getJSONObject(i)
            risk.put("identity_code",identity_code).put("user_id",data(0)).put("operator_auth_date", data(2))
          }
          val ria = risk_arr.toString()

          // risk_contact_detail
          val risk_arr_d = json.getJSONArray("risk_contact_detail")
          for (i <- 0 until risk_arr_d.length()) {
            val risk = risk_arr_d.getJSONObject(i)
            risk.put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2))
          }
          val ria_d = risk_arr_d.toString()

          // finance_contact_stats
          val finance_arr = json.getJSONArray("finance_contact_stats")
          for (i <- 0 until finance_arr.length()) {
            val finance = finance_arr.getJSONObject(i)
            finance.put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2))
          }
          val fin_arr = finance_arr.toString()

          // finance_contact_detail
          val finance_arr_d = json.getJSONArray("finance_contact_detail")
          for (i <- 0 until finance_arr_d.length()) {
            val finance = finance_arr_d.getJSONObject(i)
            finance.put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2))
          }
          val fin_arr_d = finance_arr_d.toString()

          //behavior_score
          val behavior_score = json.getJSONObject("behavior_score")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //contact_blacklist_analysis
          val contact_blacklist_analysis = json.getJSONObject("contact_blacklist_analysis")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //contact_manyheads_analysis
          val contact_manyheads_analysis = json.getJSONObject("contact_manyheads_analysis")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //contact_creditscore_analysis
          val contact_creditscore_analysis = json.getJSONObject("contact_creditscore_analysis")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //all_contact_stats
          val all_contact_stats = json.getJSONObject("all_contact_stats")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //carrier_consumption_stats
          val carrier_consumption_stats = json.getJSONObject("carrier_consumption_stats")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //active_silence_stats
          val active_silence_stats = json.getJSONObject("active_silence_stats")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          //behavior_analysis
          val behavior_analysis = json.getJSONObject("behavior_analysis")
            .put("identity_code", identity_code).put("user_id",data(0)).put("operator_auth_date", data(2)).toString

          Tuple14(uiStr,miStr,ria,ria_d,fin_arr,fin_arr_d,
            behavior_score,contact_blacklist_analysis,contact_manyheads_analysis,contact_creditscore_analysis,all_contact_stats,carrier_consumption_stats,active_silence_stats,behavior_analysis)
        } catch {
          case ex: Exception => println("user_id----->" + data(0) + "\n" + ex)
            Tuple14("empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty")
        }
      } else {
        Tuple14("empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty","empty")
      }
    })

    dataRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  /*
  解析两层嵌套Json数组外层元素
   */
  def getInfo(info: String, json: JSONObject, user_id: String, operator_auth_date: String): JSONArray ={
    val identity_code = json.get("identity_code").toString
    val user_mobile =  json.get("user_mobile").toString
    var info_arr = new JSONArray()
    try {
      val task_data = json.getJSONObject("task_data")
      if(task_data !=  null){
        info_arr =task_data.getJSONArray(info)
        val len = info_arr.length()
        if(len != 0){
          for (i <- 0 until len) {
            val info = info_arr.getJSONObject(i)
            info.putOnce("identity_code", identity_code)
            info.putOnce("user_mobile", user_mobile)
            info.putOnce("user_id", user_id)
            info.putOnce("operator_auth_date", operator_auth_date)
          }
        }
      }
    } catch {
      case ex: Exception => println("identity_code----->" + identity_code + "\n" + ex)
    }
    info_arr
  }

  /*
  解析简单的Json对象
   */
  def getSimpleInfo(info: String, json: JSONObject, user_id: String, operator_auth_date: String): JSONObject ={
    val identity_code = json.get("identity_code").toString
    val user_mobile =  json.get("user_mobile").toString
    var json_info = new JSONObject()
    try {
      val task_data = json.getJSONObject("task_data")
      json_info = task_data.getJSONObject(info)
      if(task_data.length() != 0){
        json_info.putOnce("identity_code",identity_code)
        json_info.putOnce("user_mobile",user_mobile)
        json_info.putOnce("user_id", user_id)
        json_info.putOnce("operator_auth_date", operator_auth_date)
      }
    } catch {
      case ex: Exception => println("identity_code----->" + identity_code + "\n" + ex)
    }
    json_info
  }

  /*
  解析嵌套Json数组中的Json数组
   */
  def getRecord(info: JSONArray, arr: String, cycleName: String): JSONArray ={
    val record_arr:JSONArray = new JSONArray()
    try {
      for (i <- 0 until info.length()) {
        val bill_record = info.getJSONObject(i).getJSONArray(arr)
        val identity_code = info.getJSONObject(i).get("identity_code").toString
        val user_mobile =  info.getJSONObject(i).get("user_mobile").toString
        val cycle =  info.getJSONObject(i).get(cycleName).toString
        for (m <- 0 until bill_record.length()){
          val record = bill_record.getJSONObject(m)
          record.putOnce("identity_code", identity_code)
          record.putOnce("user_mobile", user_mobile)
          record.putOnce(cycleName, cycle)
          record_arr.put(record)
        }
      }
    } catch {
      case ex: Exception => println("JSONArray----->" + arr + "\n" + ex)
    }
    record_arr
  }
}
