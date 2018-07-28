package com.lqwork.demo

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}

object AvroParse {
  def main(args: Array[String]): Unit = {
    val s = "{\"type\":\"record\", \"name\":\"Iteblog\", \"fields\":[{\"name\":\"str1\", \"type\":\"string\"},{\"name\":\"str2\", \"type\":\"string\"},{\"name\":\"int1\", \"type\":\"int\"}]}"
    val schema = new Schema.Parser().parse(s)
    val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)


    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("str1", "aaaaa")
    avroRecord.put("str2", "bbbbb")
    avroRecord.put("int1", 1000000)

    val message = recordInjection.apply(avroRecord)

//    println(recordInjection.invert(message).get)
    println(avroRecord.get("str1").toString)
    println(avroRecord)

  }
}
