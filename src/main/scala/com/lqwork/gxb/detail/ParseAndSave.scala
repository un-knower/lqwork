package com.lqwork.gxb.detail

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json.{JSONArray, JSONObject}

class ParseAndSave extends Serializable {

  /**
    * Adding user_id and idCard to each ecommerce record and saving them into Hive.
    */
  def ecommerce(spark: SparkSession, oem: String, yesterday: String): Unit = {
    val df_ecommerce = spark.read.table("gxb_" +oem+ ".gxb")
//      .where("auth_type = 'ecommerce'").select("jsonStr").rdd
      .where("etl_ts like '"+ yesterday +"%' and auth_type = 'ecommerce'").select("jsonStr")

    if (!df_ecommerce.head(1).isEmpty) {
      val rdd_ecommerce = df_ecommerce.rdd
        .mapPartitions(iter => {
          iter.map(m => {
            val arr = m.get(0).toString.split("\\^\\*\\^\\*\\^")

            val json = new JSONObject(arr(0))
            val user_id = arr(1)
            val idCard = arr(2)
            val created_at = arr(3)
            val updated_at = arr(4)

            (json, user_id, idCard, created_at, updated_at)
          })
        })

      // 用户基本信息
      val rdd_ecommerceBaseInfo = jsonObjAddUserInfo(rdd_ecommerce, "ecommerceBaseInfo")
      // 花呗信息
      val rdd_huabeiInfo = jsonObjAddUserInfo(rdd_ecommerce, "huabeiInfo")
      // 借呗信息
      val rdd_jiebeiInfo = jsonObjAddUserInfo(rdd_ecommerce, "jiebeiInfo")
      // 收货地址
      val rdd_ecommerceConsigneeAddresses = jsonArrAddUserInfo(rdd_ecommerce, "ecommerceConsigneeAddresses")
      // 交易记录
      val rdd_ecommerceTrades = jsonArrAddUserInfo(rdd_ecommerce, "ecommerceTrades")
      // 淘宝交易记录
      val rdd_taobaoOrders = jsonArrAddUserInfo(rdd_ecommerce, "taobaoOrders")


      /*spark.read.table("gxb_" +oem+ ".ecommerceBaseInfo").withColumn("taobaoImgUrl", lit(""))
        .write.mode("overwrite").format("parquet").saveAsTable("gxb_" +oem+ ".ecommerceBaseInfo_new")*/
      spark.read.json(rdd_ecommerceBaseInfo).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".ecommerceBaseInfo")
      spark.read.json(rdd_huabeiInfo).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".huabeiInfo")

      if(!rdd_jiebeiInfo.isEmpty){
        spark.read.json(rdd_jiebeiInfo).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".jiebeiInfo")
      }
      spark.read.schema(Schema.schema_ecommerceConsigneeAddresses).json(rdd_ecommerceConsigneeAddresses)
        .repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".ecommerceConsigneeAddresses")
      spark.read.schema(Schema.schema_ecommerceTrades).json(rdd_ecommerceTrades).where("user_id is not null")
        .repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".ecommerceTrades")
      spark.read.schema(Schema.schema_taobaoOrders).json(rdd_taobaoOrders).where("user_id is not null")
        .repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".taobaoOrders")
    } else {
      println("Missing ecommerce data at "+ yesterday +" ! ! !")
    }

  }


  /**
    * Adding user_id and idCard to each jd record and saving them into Hive.
    */
  def jd(spark: SparkSession, oem: String, yesterday: String):Unit ={
    val df_jd = spark.read.table("gxb_" +oem+ ".gxb")
      //      .where("auth_type = 'jd'").select("jsonStr").rdd
      .where("etl_ts like '"+ yesterday +"%' and auth_type = 'jd'").select("jsonStr")
    if (!df_jd.head(1).isEmpty) {
      val rdd_jd = df_jd.rdd
        .mapPartitions(iter => {
          iter.map(m => {
            val arr = m.get(0).toString().split("\\^\\*\\^\\*\\^")

            val json = new JSONObject(arr(0))
            val user_id = arr(1)
            val idCard = arr(2)
            val created_at = arr(3)
            val updated_at = arr(4)

            (json, user_id, idCard, created_at, updated_at)
          })
        })/*.persist(StorageLevel.MEMORY_AND_DISK)*/

      // 账户信息
      val rdd_accountInfo = jsonObjAddUserInfo(rdd_jd, "accountInfo")

      // 认证信息
      val rdd_certificationInfo = jsonObjAddUserInfo(rdd_jd, "certificationInfo")

      // 绑卡信息
      val rdd_bankCardList = jsonArrAddUserInfo(rdd_jd, "bankCardList")

      // 地址信息
      val rdd_addressList = jsonArrAddUserInfo(rdd_jd, "addressList")

      // 登陆信息
      val rdd_loginHistoryList = jsonArrAddUserInfo(rdd_jd, "loginHistoryList")

      // 发票信息
      val rdd_invoiceList = jsonArrAddUserInfo(rdd_jd, "invoiceList")

      // 白条账单信息
      val rdd_btBillList = jsonArrAddUserInfo(rdd_jd, "btBillList")

      // 白条还款信息
      val rdd_btRepayInfoList = jsonArrAddUserInfo(rdd_jd, "btRepayInfoList")

      // 购物订单信息
      val rdd_orderList = jsonArrAddUserInfo(rdd_jd, "orderList")

      spark.read.json(rdd_accountInfo).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".accountInfo")
      spark.read.json(rdd_certificationInfo).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".certificationInfo")
      spark.read.json(rdd_bankCardList).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".bankCardList")
      spark.read.json(rdd_addressList).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".addressList")
      spark.read.json(rdd_loginHistoryList).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".loginHistoryList")
      spark.read.json(rdd_invoiceList).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".invoiceList")
      spark.read.json(rdd_btBillList).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".btBillList")
      spark.read.json(rdd_btRepayInfoList).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".btRepayInfoList")
      spark.read.json(rdd_orderList).repartition(1).write.mode("append").format("parquet").saveAsTable("gxb_" +oem+ ".orderList")
    } else {
      println("Missing jd data at "+ yesterday +" ! ! !")
    }
    
  }


  /**
    * Adding user_id and idCard to JSONObject.
    */
  def jsonObjAddUserInfo (rdd: RDD[(JSONObject, String, String, String, String)], key: String): RDD[String] ={
    rdd.map(m => {
      try{
        val jsonObj = m._1.getJSONObject(key)
        if (jsonObj.length() != 0) {
          jsonObj.put("user_id", m._2)
          jsonObj.put("idCard", m._3)
          jsonObj.put("created_at", m._4)
          jsonObj.put("updated_at", m._5)
        }

        jsonObj
      } catch {
        case e: Exception => println(m._2 + "------>" + e)
          new JSONObject()
      }
    }).filter(_.length() != 0).map(_.toString)

  }

  
  /**
    * Adding user_id and idCard to JSONArray.
    */
  def jsonArrAddUserInfo (rdd: RDD[(JSONObject, String, String, String, String)], key: String): RDD[String] ={
    rdd.map(m => {
      try{
        val jsonArr = m._1.getJSONArray(key)
        if (jsonArr.length() != 0) {
          for (i <- 0 until jsonArr.length()) {
            val ele = jsonArr.getJSONObject(i)
            ele.put("user_id", m._2)
            ele.put("idCard", m._3)
            ele.put("created_at", m._4)
            ele.put("updated_at", m._5)
          }
        }

        jsonArr
      } catch {
        case e:Exception => println(m._2 + "------>" + e)
        new JSONArray()
      }
    }).filter(_.length() != 0).map(_.toString)
  }
  
}