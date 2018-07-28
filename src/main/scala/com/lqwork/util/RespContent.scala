package com.lqwork.util

object RespContent {
  def getReportV2(task_id: String): String = {
    val httpUtil = new HttpUtil_V2
    val paramStr = "task_id=" + task_id
    httpUtil.sendHttpRequest("shield.gateway", "shield.report", paramStr, "POST").get("respContent").toString
  }

  def getReportV4(task_id: String): String = {
    val httpUtil = new HttpUtil
    val paramStr = "task_id=" + task_id
    httpUtil.sendHttpRequest("shield.gateway", "shield.report", paramStr, "POST").get("respContent").toString()
  }
}
