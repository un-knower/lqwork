package com.lqwork.demo

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.mongodb.hadoop.MongoInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.AccumulatorV2
import org.bson.BSONObject
import org.json.{JSONArray, JSONObject}

object SparkLearn {
  val spark = SparkSession.builder().appName("Spark_Learning").master("local[4]").getOrCreate()
//  val spark = SparkSession.builder().appName("Spark_Learning").enableHiveSupport().getOrCreate()
  import spark.implicits._

  def main(args: Array[String]): Unit = {
//    sparkSQL(spark)
//    rdd(spark)
    mongodb(spark)
    spark.close()
  }

  def rdd(spark: SparkSession): Unit ={
    var acc = spark.sparkContext.longAccumulator("acc")
    val sum = spark.sparkContext.parallelize(Array(1,2,3,4,5)).map(m => {acc.add(1L)}).cache()
    sum.count()
    sum.first()
    println(acc.value)

//    spark.sparkContext.textFile("F:/rdd.txt").flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)
//    val rdd         = spark.sparkContext.textFile("F:\\rdd.txt")
//    val flatMap     = rdd.flatMap(line => line.split(","))
//    val map         = flatMap.map(word => (word, 1))
//    val reduceByKey = map.reduceByKey(_ + _)
//    val foreach     = reduceByKey.foreach(println)
  }

  def sparkSQL(spark: SparkSession): Unit ={


/*

//    val jsonStr = Map("1000" -> "{'name':'阿木','date':2}")
    val jsonStr = Map(
      "1000" -> "{'name':'zhangsan','age':null}",
      "2000" -> "{'name':'zhangsan','age':10}",
      "4000" -> "{'name':'zhangsan','age':10}",
      "5000" -> "{'name':'lisi','age':20}",
      "6000" -> "{'name':'lisi','age':20}",
      "7000" -> "{'name':'wangwu','age':20}",
      "8000" -> "{'name':'zhaoliu','age':50}",
      "9000" -> "{'name':'zhaoliu','age':40}",
      "10000" -> "{'name':'zhaoliu','age':20}"
    )
    val jsonRDD = spark.sparkContext.makeRDD(jsonStr.values.toSeq)
    spark.read.json(jsonRDD).select($"name", split($"phone", ",").alias("phone")).where($"phone" < 2)
      .show(false)/*.where(length($"phone") > 1)*/

    spark.read.json(jsonRDD)
      .groupBy($"name", $"age").agg("age" -> "count").withColumnRenamed("count(age)", "cnt")
      .select($"name", $"age", row_number().over(Window.partitionBy("name").orderBy($"cnt".desc)).alias("rn"))
      .where($"rn".<=(2)).show()

    spark.read.json(jsonRDD).na.fill(0)
      .show(false)
*/


    /*spark.read.json(jsonRDD).createOrReplaceTempView("tmp")
    spark.sql("select name,age from tmp").show(false)*/


    //    spark.catalog.refreshTable("demo.gxb_new")
    //    spark.sql("refresh table demo.gxb_new")
    //    spark.sql("select auth_type,etl_ts from demo.gxb limit 1").write.mode("append").format("parquet").saveAsTable("demo.gxb_new")

    /*val people = spark.read.parquet("").as[Person]
    val names = people.map(_.name)
    val ageCol = people("age") + 10
    Seq(1, 2, 3).toDS().show()*/

    /*val df = spark.createDataFrame(Seq(("2018-01-22 00:00:00", "Spark Hadoop Hbase Hive Kafka Storm", 3)))
      .withColumnRenamed("_1", "date").withColumnRenamed("_2", "skill").withColumnRenamed("_3", "num")
        df.explain()//Prints the physical plan to the console for debugging purposes.
        println(df.dtypes(0))// Returns all column names and their data types as an array.
        println(df.columns(0))//Returns all column names as an array.
        println(df.isStreaming)
        df.show(1, false)
        df.na.drop().show(false)
        df.sort($"date".desc).show()
        println(df.apply("date"))
        df.as("df_")
        df.select(expr("num + 1").as[Int]).show(false)
        df.filter("num > 1").show(false)
        df.groupBy($"num").avg().show(false)
        df.groupBy($"num").agg(Map("num" -> "max")).show(false)
        df.select('name, explode(split('age, " ")).as("age"))*/


    /*val jsonStr = Map("1000" -> "{'name':'阿木','phone':'18312366493'}")
    val jsonRDD = spark.sparkContext.makeRDD(jsonStr.get("1000").toSeq)
    spark.read.json(jsonRDD).show(false)*/

    /*val t = Tuple2(id, project_id)
    val rdd = spark.sparkContext.makeRDD(Seq(t))
    rdd.toDS().withColumnRenamed("_1", "id").withColumnRenamed("_2", "project_id").show()
    println(rdd.toDS())*/


    /*
    //方法四
    val df = spark.createDataFrame(jsonStr.toSeq).select($"_1", json_tuple(($"_2").cast("string"), "$.name", "$.phone")).show(false)

    //方法三
    spark.createDataFrame(jsonStr.toSeq)
      .map(m => {
        val json = new JSONArray(m.get(1))
        val len = json.length()
        for(i <- 0 until len){

        }
      }).show(false)

    //方法二
    val jsonRDD = spark.sparkContext.makeRDD(jsonStr.get("1000").toSeq)
    spark.read.json(jsonRDD).show(false)

    //方法一
      val schema = StructType(
        Array(
          StructField("name", StringType, true),
          StructField("phone", StringType, true)
        )
      )
      val df = spark.createDataFrame(jsonStr.toSeq).select($"_1",
        explode(
          split(
            substring_index(
              substring_index(
                regexp_replace($"_2", "},", "}^^^"),
                "[", -1),
              "]", 1),
            "\\^\\^\\^")
        ).as("_2")
      )
    df.select($"_1",
      from_json($"_2", schema).getField("name").as("contacts_name"),
      from_json($"_2", schema).getField("phone").as("contacts_phone")
    ).show(false)*/



    /*
    //时间函数
    spark.createDataFrame(Seq(("2018-01-22 00:00:00", 2, 3))).select("date").withColumn("date", substring($"date", 0, 7)).withColumn("date", date_sub(concat(substring($"date", 0, 8), lit("15 00:00:00")), 30))
    LocalDate.parse("2018-01-22 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss")).minusMonths(1)*/
  }

  def mongodb(spark: SparkSession): Unit ={
    val conf = new Configuration()
    conf.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat")
    conf.set("mongo.auth.uri", "mongodb://caiwu:Nndm59U4FqdhECMg@dds-bp1b5378b1cf06842369-pub.mongodb.rds.aliyuncs.com/admin")
    conf.set("mongo.input.uri", "mongodb://dds-bp1b5378b1cf06842369-pub.mongodb.rds.aliyuncs.com:3717/sdhh.contacts")
    val fClass = classOf[com.mongodb.hadoop.MongoInputFormat]
    val kClass = classOf[Object]
    val vClass = classOf[BSONObject]
    val rdd = spark.sparkContext.newAPIHadoopRDD(conf, fClass, kClass, vClass).repartition(1)/*.saveAsTextFile("F:\\files")*/
    println(rdd.count())

  }

  case class Person(name: String, age: String)
}
