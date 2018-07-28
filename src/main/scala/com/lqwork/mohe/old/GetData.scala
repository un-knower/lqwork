package com.lqwork.mohe.old

import com.lqwork.util.{DecompressJson, RespContent}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.json.JSONObject

import scala.collection.mutable.ListBuffer

object GetData {

  def getOperateData(spark:SparkSession, occur_time:String, task_ids:Array[Row], operHDFSPath: String): Unit ={
    import spark.implicits._
    try {
      val list = new ListBuffer[String]
      val len = task_ids.size
      val in = len / 100
      var result = ""
      for (i <- 0 until in) {
        for (j <- i * 100 until (i + 1) * 100) {
          try {
            val task_id_arr = task_ids(j)(0).toString().split("\\^")
            val obj = new JSONObject(RespContent.getReportV4(task_id_arr(1)))
            val data = obj.getJSONObject("data").toString()
            result = task_id_arr(0) + "^" + data + "^" + task_id_arr(2)
              list.append(result)
          } catch {
            case e: Exception => println("json get data error " + e)
          }
        }
        spark.sparkContext.makeRDD(list).coalesce(1).toDF().write.mode("append").text(operHDFSPath + occur_time)
        list.clear()
      }

      for (i <- in * 100 until len) {
        val task_id_arr = task_ids(i)(0).toString().split("\\^")
        val obj = new JSONObject(RespContent.getReportV4(task_id_arr(1)))
        val data = obj.getJSONObject("data").toString()
        result = task_id_arr(0) + "^" + data + "^" + task_id_arr(2)
        list.append(result)
      }
      spark.sparkContext.makeRDD(list).toDF().coalesce(1).write.mode("append").text(operHDFSPath + occur_time)
      list.clear()
    } catch {
      case e: Exception => println(e)
    }
  }

  def getFinanceData(task_ids:Array[Row], spark:SparkSession, occur_time:String, finHDFSPath: String): Unit ={
    import spark.implicits._
    try {
      val list = new ListBuffer[String]
      val len = task_ids.size
      val in = len / 100
      var result = ""
      for (i <- 0 until in) {
        for (j <- i * 100 until (i + 1) * 100) {
          val task_id_arr = task_ids(j)(0).toString().split("\\^")
          val obj = new JSONObject(RespContent.getReportV2(task_id_arr(1)))
          val data = obj.get("data").toString()
          if (StringUtils.isNotBlank(data)) {
            val data_json = DecompressJson.gunzip(data)
            result = task_id_arr(0) + "^" + data_json + "^" + task_id_arr(2)
            list.append(result)
          }
        }
        spark.sparkContext.makeRDD(list).toDF().coalesce(1).write.mode("append").text(finHDFSPath + occur_time)
        list.clear()
      }

      for (i <- in * 100 until len) {
        val task_id_arr = task_ids(i)(0).toString().split("\\^")
        val obj = new JSONObject(RespContent.getReportV2(task_id_arr(1)))
        val data = obj.get("data").toString()
        if(StringUtils.isNotBlank(data)){
          val data_json = DecompressJson.gunzip(data)
          result = task_id_arr(0) + "^" + data_json + "^" + task_id_arr(2)
          list.append(result)
        }
      }
      spark.sparkContext.makeRDD(list).toDF().coalesce(1).write.mode("append").text(finHDFSPath + occur_time)
      list.clear()
    } catch {
      case e: Exception => println(e)
    }
  }
}
