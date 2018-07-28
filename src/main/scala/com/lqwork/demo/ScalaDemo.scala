package com.lqwork.demo

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZonedDateTime}
import java.util.regex.{Matcher, Pattern}

import jodd.util.URLDecoder
import org.apache.spark.sql.SparkSession
import org.json.{JSONArray, JSONObject}

import scala.io.Source._

object ScalaDemo {
  private var json = new JSONObject()
  def main(args: Array[String]): Unit = {

    //println("a*b".split("\\*")(0))
    //println("a-b".split("-")(0))


    /*val url = "http://jybk-2.oss-cn-hangzhou-internal.aliyuncs.com/yys/20170921/TASKYYS100000201709211236070700791569_report.txt".replace("-internal", "")
    //println(new JSONObject(fromURL(url, "utf-8").mkString).getJSONObject("ecommerceBaseInfo"))
    println(new JSONObject(fromURL(url, "utf-8").mkString))*/


    /*val str = "[{'name': 'hh', 'age': 10}, {'name': 'kk', 'age': 20}]"
    val jsonArray = new JSONArray(str)
    for (i <- 0 until jsonArray.length()) {
      jsonArray.getJSONObject(i).remove("name")
      jsonArray.getJSONObject(i).put("sex", "0")
      //jsonArray.getJSONObject(i).remove("name").asInstanceOf[JSONObject].put("age", 10)
    }
    println(jsonArray)
    val str = "{}"
    val str2 = "[]"
    println(new JSONObject(str).length())
    println(new JSONArray(str2).length())*/


    /*println(LocalDate.now().minusDays(22).toString)
    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
    println(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    println(ZonedDateTime.now())*/



    /*var str = "\\u5ba2\\u6237\\u884c\\u4e3a\\u68c0\\u6d4b"
    println(unicodeToString(str))*/

    /*val j1 = singleJson("{'name':'hei'}")
    val j2 = singleJson("{'name':'hei'}")
    println(j1 == j2)*/

    /*val seq = for (i <- 1 to 10) yield i * 2
    println(seq)*/

    //println("ab".split(",").length)

    //println(Seq(1, 2, 3).updated(0, 10))

    /*val json = "{'jiebei': []}"
    println(new JSONObject(json).getJSONObject("jiebei").put("a", 100))
    println(new JSONObject(json).getJSONObject("jiebei").length())*/

    //println(15 ^ 2)
    //println(11 >> 1)

    /*val j = "{'id': 735354,'ts_ms': 1524634965148}"
    println(new JSONObject(j).getLong("ts_ms"))*/

    /*val list = List(1, 2, 3)
    println(list.head)
    println(list.tail)
    println(list.last)
    println(list :+ 4)
    println(4 +: list)
    println(list.::(4))*/

    /*val spark = SparkSession.builder().appName("").master("local[4]").getOrCreate()
    val s = ""
    new JSONObject(s)
    val rdd = spark.sparkContext.makeRDD(Seq(s))
    spark.read.json(rdd).show(1)
    spark.close()*/
  }

  def unicodeToString(str: String): String = {
    var s = str
    val pattern = Pattern.compile("(\\\\u(\\p{XDigit}{4}))")
    val matcher = pattern.matcher(s)
    var ch = 'a'
    while ( {matcher.find}) {
      ch = Integer.parseInt(matcher.group(2), 16).toChar
      s = s.replace(matcher.group(1), ch + "")
    }
    s
  }

  def singleJson(jsonStr: String) ={
    json = new JSONObject(jsonStr)
    json
  }
}
