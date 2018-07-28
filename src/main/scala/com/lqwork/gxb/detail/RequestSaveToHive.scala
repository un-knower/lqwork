package com.lqwork.gxb.detail

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.Source._

class RequestSaveToHive {
  /*
  Request json data from URL and save it to Hive.
   */
  def requestAndSave(spark: SparkSession, oem: String, yesterday: String): Unit ={
    import spark.implicits._

    /*val df_gxb = spark.read.table("buffer.buffer_" +oem+ "_gxb_authorizations")
      .select($"user_id", $"report_addr", $"auth_type", $"created_at", $"updated_at")
      .where("auth_status = 1 and report_addr != ''")
    val df_card = spark.read.table("buffer.buffer_" +oem+ "_cards").select($"user_id", $"idcard")*/

    val df_gxb = spark.read.table("buffer.buffer_" +oem+ "_gxb_authorizations")
      .where("report_addr != '' and report_addr is not null and auth_status = 1")
      .where("created_at like '" + yesterday + "%' or updated_at like '" + yesterday + "%'")
      .select($"user_id", $"report_addr", $"auth_type", $"created_at", $"updated_at")
    val df_card = spark.read.table("buffer.buffer_" +oem+ "_cards")
      .select($"user_id", $"idcard")

    if (!df_gxb.head(1).isEmpty) {
      val df_url = df_gxb.join(df_card, Seq("user_id"), "left_outer")
        .select(concat_ws("^*^*^", $"report_addr", $"user_id", $"idcard", $"created_at", $"updated_at"), $"auth_type")

      val rdd = df_url.rdd.repartition(20).mapPartitions(iter => {
        iter.map(m => {
            val arr = m.get(0).toString().split("\\^\\*\\^\\*\\^")

            val json = fromURL(arr(0), "utf-8").mkString
            val jsonStr = json +"^*^*^"+ arr(1) +"^*^*^"+ arr(2) +"^*^*^"+ arr(3) +"^*^*^"+ arr(4)
            val auth_type = m.get(1).toString

            Row(jsonStr, auth_type)
        })
      })

      val schema = StructType(
        StructField("jsonStr", StringType, false)::
          StructField("auth_type", StringType, false):: Nil
      )

      //The time records is saved to Hive.
      spark.createDataFrame(rdd, schema)
        .withColumn("etl_ts", lit(yesterday))
        .repartition(1)
        .write
        .mode("append")
        .format("parquet")
        .saveAsTable("gxb_" +oem+ ".gxb")
    } else {
      println(oem + "_gxb_authorizations has not data at "+ yesterday +" ! ! !")
    }

  }
}