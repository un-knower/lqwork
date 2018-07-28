package com.lqwork.common

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.json.{JSONArray, JSONObject}

class Utils extends Serializable {
  /**
    * Get base_url from gxb table.
    */
  def getGXBBaseURL(spark:SparkSession, oem: String): Dataset[Row] ={
    val df_gxb = spark.read.table("buffer.buffer_" +oem+ "_gxb_authorizations")
      .where("updated_at like '2018-07-10%'")
      .where("report_addr != '' and report_addr is not null and report_addr != 'null' and auth_status = 1")
      .select("user_id", "report_addr", "updated_at", "report_type_id", "auth_type")
    val df_card = spark.read.table("buffer.buffer_" +oem+ "_cards")
      .select("user_id", "idcard")
    df_gxb.join(df_card, Seq("user_id"), "left_outer")
      .select("user_id", "idcard", "report_addr", "updated_at", "report_type_id", "auth_type")
  }

  /**
    * Get base_url from gxb table.
    */
  def getGXBReportURL(spark:SparkSession, oem: String): Dataset[Row] ={
    val df_gxb = spark.read.table("buffer.buffer_" +oem+ "_gxb_authorizations")
      .where("updated_at like '2018-07-10%'")
      .where("report_final_url != '' and auth_status = 1 and auth_type = 'ecommerce'")
      .select("user_id", "report_final_url", "updated_at")
    val df_card = spark.read.table("buffer.buffer_" +oem+ "_cards")
      .select("user_id", "idcard")
    df_gxb.join(df_card, Seq("user_id"), "left_outer")
      .select("user_id", "idcard", "report_final_url", "updated_at")
  }
  
  /**
    * Get base_url from operator table.
    */
  def getBaseURL(spark:SparkSession, oem: String): Dataset[Row] ={
    spark.read.table("buffer.buffer_" +oem+ "_operator")
      .where("updated_at like '2018-07-18%'")
      .select("user_id", "base_url", "updated_at")
      .where("base_url != '' and base_url is not null and base_url != 'null'")
  }

  /**
    * Get report_url from operator table.
    */
  def getReportURL(spark:SparkSession, oem: String): Dataset[Row] ={
    spark.read.table("buffer.buffer_" +oem+ "_operator")
      .where("updated_at like '2018-07-18%'")
      .select("user_id", "report_url", "updated_at")
      .where("report_url != '' and report_url is not null and report_url != 'null'")
  }

  /**
    * Add k-v into unnested JSONArray.
    */
  def jsonArrayAddKV(jsonArr: JSONArray, user_id: String, identity_code:String, updated_at: String): JSONArray ={
    val len = jsonArr.length()
    if (len > 0) {
      for (i <- 0 until len) {
        jsonObjectAddKV(jsonArr.getJSONObject(i),user_id,identity_code,updated_at)
      }
    }
    jsonArr
  }

  /**
    * Add k-v into JSONObject.
    */
  def jsonObjectAddKV(json: JSONObject, user_id: String, identity_code:String, updated_at: String): JSONObject ={
    if (json.length() > 0) {
      json.putOnce("user_id", user_id)
      json.putOnce("identity_code", identity_code)
      json.putOnce("updated_at", updated_at)
    }
    json
  }
}