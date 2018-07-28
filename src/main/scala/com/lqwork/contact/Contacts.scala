package com.lqwork.contact

import java.time.LocalDate

import com.lqwork.common.SparkSessionInit
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.json.{JSONArray, JSONObject}

object Contacts {
  val spark = SparkSessionInit.initSession("Contacts")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    /*takeContacts(spark, "baobao")
      .union(takeContacts(spark, "msdq"))
      .union(takeContacts(spark, "jybk"))
      .union(takeContacts(spark, "yrd"))
      .union(takeContacts(spark, "xzbk"))
      .union(takeContacts(spark, "xzbk2"))
      .union(takeContacts(spark, "aj"))
      .union(takeContacts(spark, "ddh"))
      .union(takeContacts(spark, "ddhs"))
      .union(takeContacts(spark, "hjh"))
      .union(takeContacts(spark, "hjsj"))
      .union(takeContacts(spark, "jm"))
      .union(takeContacts(spark, "yhd"))
      .union(takeContacts(spark, "ymbk"))
      .union(takeContacts(spark, "sdhh"))
      .union(takeContacts(spark, "kb"))
      .union(takeContacts(spark, "bb"))
      .union(takeContacts(spark, "lxb"))
//      .union(takeContacts(spark, "lxbh5"))
//      .groupBy("user_id", "contacts_phone", "oem").agg(max("contacts_name").alias("contacts_name"))
      .withColumn("etl_ms", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))).coalesce(30)
      .write.mode("overwrite").format("parquet").partitionBy("oem").saveAsTable("ods.ods_allContacts_test")*/
//    val arr = Array("baobao","msdq","jybk","yrd","xzbk","xzbk2","aj","ddh","ddhs","hjh","hjsj","jm","yhd","ymbk","sdhh","kb","bb","lxb","lxbh5","lxb2","sdhhh5")
    val arr = Array("aj","ddh","ddhs","hjh","hjsj","jm","yhd","ymbk","sdhh","kb","bb","lxb","lxbh5","lxb2","sdhhh5")
    for (oem <- arr) {
      takeContacts(spark, oem)
        .groupBy("user_id", "contacts_phone", "oem").agg(max("contacts_name").alias("contacts_name"))
        .withColumn("etl_ms", lit(date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))).coalesce(5)
        .write.mode("append").format("parquet").partitionBy("oem").saveAsTable("ods.ods_allContacts")
    }

//    spark.read.table("ods.ods_allContacts").where("oem = 'baobao' or oem = 'msdq' or oem = 'jybk' or oem = 'yrd' or oem = 'xzbk' or oem = 'xzbk2'")
//      .write.mode("append").format("parquet").partitionBy("oem").saveAsTable("ods.ods_allContacts_zl")
    spark.close()
  }

  /**
    * 解析Json通讯录
    */
  private def takeContacts(spark: SparkSession, oem: String): Dataset[Row] = {
    val schema_zl = StructType(
      StructField("name", StringType, true) ::
      StructField("phone", StringType, true) :: Nil
    )
    val schema_hs = StructType(
      StructField("name", StringType, true) ::
      StructField("tel", StringType, true) :: Nil
    )
    val schema_urgent = StructType(
      StructField("name", StringType, true) ::
      StructField("relation", IntegerType, true) ::
      StructField("phone", StringType, true) :: Nil
    )

    val dayStr = LocalDate.now()
    var df_tmp = spark.emptyDataFrame
    if (Array("baobao","msdq","jybk","yrd","xzbk","xzbk2").contains(oem)) {
      //读取通讯录表
      val df_zl = spark.read.table("buffer.buffer_" + oem + "_contacts").select($"user_id", $"contacts")
        .where("contacts != '[]'")
//      df_zl.select($"user_id", from_json($"contacts", schema)).show()

      /*val rdd = df_zl.select($"user_id", $"contacts").rdd.map(m => {
        Row(m.get(0), new JSONArray(m.get(1).toString))
      })
      val df = spark.createDataFrame(rdd, schema)
      df.show(false)
      df.printSchema()*/

      //解析Json格式的通讯录，一行转多行
      val df_zl_detail = explodeFunc(df_zl).where("contact like '%name%phone%'")
        .select($"user_id",
          from_json($"contact", schema_zl).getField("name").as("contacts_name"),
          from_json($"contact", schema_zl).getField("phone").as("contacts_phone")
        )

      // 过滤非正常电话号码
      df_tmp = filterContact(df_zl_detail, oem)
    }
    else if (Array("aj","ddh","ddhs","hjh","hjsj","jm","yhd","ymbk","sdhh","kb","bb","lxb","lxb2").contains(oem)) {
      //读取存储在HDFS上的, 从MongoDB数据库导出的Json格式的通讯录Collection文件
      val rdd = spark.sparkContext.textFile("/txt/mongo/" + oem + ".json").mapPartitions(iter => {
        iter.map(m =>{
          val json = new JSONObject(m)
          val user_id = json.getInt("user_id")
          val contacts = json.get("contacts").toString
          Row(user_id, contacts)
        })
      })
      //定义schema
      val schema = StructType(
        StructField("user_id", IntegerType, false)::
        StructField("contacts", StringType, false)::Nil
      )
      //RDD转DataFrame
      val df_hs = spark.createDataFrame(rdd, schema)

      //解析Json格式的通讯录，一行转多行
      val df_hs_detail = explodeFunc(df_hs).where("contact like '%name%tel%'")
        .select($"user_id",
          from_json($"contact", schema_hs).getField("name").as("contacts_name"),
          from_json($"contact", schema_hs).getField("tel").as("contacts_phone")
        )
      // 过滤非正常电话号码
      df_tmp = filterContact(df_hs_detail, oem)
    } else if (Array("lxbh5").contains(oem)) {
      //读取紧急联系人表
      val df_urgent = spark.read.table("buffer.buffer_" + oem + "_urgent_contact").select($"user_id", $"contact".alias("contacts"))
        .where("contacts != '[]'")
      //解析Json, 一行转多行
      val df_urgent_detail = explodeFunc(df_urgent).where("contact like '%name%relation%phone%'")
        .select($"user_id",
          from_json($"contact", schema_urgent).getField("name").as("contacts_name"),
          from_json($"contact", schema_urgent).getField("phone").as("contacts_phone")
        )
      // 过滤非正常电话号码
      df_tmp = filterContact(df_urgent_detail, oem)
    }
    else {
      println("No such " + oem)
    }
    df_tmp.persist(StorageLevel.MEMORY_AND_DISK)
  }

  /**
    * 行转列
    */
  private def explodeFunc(df: Dataset[Row]): Dataset[Row] ={
    df.select($"user_id",
      explode(
        split(
          substring_index(
            substring_index(
              regexp_replace($"contacts", "},", "}^*^*"),
              "[", -1),
            "]", 1),
          "\\^\\*\\^\\*"
        )
      ).as("contact")
    )
  }

  /**
    * 过滤非正常号码
    */
  private def filterContact(df: Dataset[Row], oem: String): Dataset[Row] = {
    //过滤通讯录中没有姓名的和没有电话的联系人, 对于有多于3个电话的联系人，只取前3个
    val rdd = df.where("contacts_name is not null and contacts_name != '' and contacts_phone is not null and contacts_phone != ''").rdd
      .mapPartitions(iter => {
        iter.map(m => {
          val user_id = m.get(0)
          val contacts_name = m.get(1)
          val phone_arr = m.get(2).toString.split(",")
          var contacts_phone = m.get(2).toString
          if (phone_arr.length > 3) {
            contacts_phone = phone_arr(0) +","+ phone_arr(1) +","+ phone_arr(2)
          }
          Row(user_id, contacts_name, contacts_phone)
        })
      })

      val schema = StructType(
        StructField("user_id", IntegerType, false)::
        StructField("contacts_name", StringType, false)::
        StructField("contacts_phone", StringType, false)::Nil
      )
      spark.createDataFrame(rdd, schema)
        .select($"user_id", $"contacts_name", explode(split($"contacts_phone", ",")).alias("contacts_phone")).withColumn("oem", lit(oem))
  }

  /**
    * Rent contacts
    * @param spark
    * @param oem
  def parseJosnContacts(spark: SparkSession, oem: String): Unit ={
    import spark.implicits._

    val schema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("phone", StringType, true)
      )
    )

    val occur_time = LocalDate.now()
    val df_contact = spark.read.table("buffer.buffer_"+ oem +"_contacts").where("updated_at like '" + occur_time.toString + "%' ")
//    val df_contact = spark.read.table("buffer.buffer_"+ oem +"_contacts")
      .where("contacts != '[]'")
      .select($"user_id",
        explode(
          split(
            substring_index(
              substring_index(
                regexp_replace($"contacts", "},", "}^*^*"),
                "[", -1),
              "]", 1),
            "\\^\\*\\^\\*")
        ).as("contact")
      )
      .where("contact like '%name%' and contact like '%phone%'")
      .select($"user_id",
        from_json($"contact", schema).getField("name").as("contacts_name"),
        from_json($"contact", schema).getField("phone").as("contacts_phone")
      )

    val df_base = spark.read.table("ods.ods_zl_users").where("oem = '"+ oem +"'").select($"user_id", $"identity_code", $"user_phone")

    df_base.join(df_contact, Seq("user_id"), "left_outer")
      .withColumn("oem", lit(oem))
      .withColumn("plat", lit("pgd"))
      .withColumn("occur_time", lit(occur_time.minusDays(1).toString))
      .write.mode("append").partitionBy("oem").format("parquet").saveAsTable("ods.ods_contacts_details")

  }

  /**
    * Some user's contacts
    * @param spark
  def parseJosnContactsKLD(spark: SparkSession): Unit ={
    import spark.implicits._

    val schema = StructType(
      Array(
        StructField("name", StringType, true),
        StructField("phone", StringType, true)
      )
    )

    val occur_time = LocalDate.now()
    val df = spark.read.table("klduser.useraddressbook")
      .where("uuid in ('bd5b91b022764813aeaa757c30362732', 'ed9a82dbefe34aa8a9ff6507575aa727', '23fb9bd909db4794b76c2aa5f3b2a840', '34ced46b92474122b8fd356aa59511b2', '0d1cea6383b548f48e2bf35326ce8f3e', '186188f40ec54d498f569371a425f389', '141f4b08264d433e952a001d9b8f2cd4', '00919c38868b498b8856fdc72a627f97')")
      //    val df_contact = spark.read.table("buffer.buffer_"+ oem +"_contacts")
      .where("contacts != '[]'")
      .select($"uuid",
        explode(
          split(
            substring_index(
              substring_index(
                regexp_replace($"contacts", "},", "}^*^*"),
                "[", -1),
              "]", 1),
            "\\^\\*\\^\\*")
        ).as("contact")
      )
      .where("contact like '%name%' and contact like '%phone%'")
      .select($"uuid",
        from_json($"contact", schema).getField("name").as("contacts_name"),
        from_json($"contact", schema).getField("phone").as("contacts_phone")
      )
      .write.mode("append").saveAsTable("user_label.contact_tmp")

  }*/*/
}
/*
/home/hadoop/app/spark-2.1.0/bin/spark-submit \
--class com.lqwork.contact.Contacts \
--master spark://10.1.11.61:7077 \
--executor-memory 20G \
--total-executor-cores 30 \
/opt/taf/jar/lqwork.jar
 */