package com.lqwork.gxb

import java.security.MessageDigest
import java.util.UUID

import scalaj.http.Http

object EcommerceReport {
  def httpRequest(name: String, phone: String, idcard: String, auth_item: String): Unit ={
    val app_id = "gxb670e487895dc1474"
    val app_security = "e99554c4ecd24beb826a16f276c8c4e1"
    val timestamp = System.currentTimeMillis().toString
    val sequence_no = UUID.randomUUID().toString.replace("-", "")
    val sign = MessageDigest.getInstance("MD5").digest((app_id + app_security + auth_item + timestamp + sequence_no).getBytes())

    val data = "{" +
      "\"appId\": "+app_id+"," +
      "\"sign\": "+sign+"," +
      "\"sequenceNo\": "+sequence_no+"," +
      "\"authItem\": "+auth_item+"," +
      "\"timestamp\": "+timestamp+"," +
      "\"name\": "+name+"," +
      "\"phone\": "+phone+"," +
      "\"idcard\": "+idcard+"}"

    val response = Http("https://prod.gxb.io/crawler/auth/v2/get_auth_token")
      .postData(data)
      .header("Content-Type", "application/json")
      .asString.code
    println(response)
  }

  def main(args: Array[String]): Unit = {
    httpRequest("周宝童","17625641001","342601198810011939","ecommerce")
  }
}
