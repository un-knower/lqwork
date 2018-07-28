package com.lqwork.gxb.detail

import org.apache.spark.sql.types._

object Schema {

  val schema_ecommerceConsigneeAddresses = StructType(
    StructField("user_id", StringType, false) ::
    StructField("idCard", StringType, true) ::
    StructField("address", StringType, true) ::
    StructField("postCode", StringType, true) ::
    StructField("receiveName", StringType, true) ::
    StructField("region", StringType, true) ::
    StructField("telNumber", StringType, true) ::
    StructField("uuid", StringType, true) ::
    StructField("created_at", StringType, true) ::
    StructField("updated_at", StringType, true) :: Nil
  )

  val schema_ecommerceTrades = StructType(
    StructField("user_id", StringType, false) ::
    StructField("idCard", StringType, true) ::
    StructField("amount", DoubleType, true) ::
    StructField("isDelete", IntegerType, true) ::
    StructField("otherSide", StringType, true) ::
    StructField("title", StringType, true) ::
    StructField("tradeDetailUrl", StringType, true) ::
    StructField("tradeNo", StringType, true) ::
    StructField("tradeStatusName", StringType, true) ::
    StructField("tradeTime", StringType, true) ::
    StructField("txTypeId", IntegerType, true) ::
    StructField("txTypeName", StringType, true) ::
    StructField("created_at", StringType, true) ::
    StructField("updated_at", StringType, true) :: Nil
  )

  val schema_taobaoOrders = StructType(
    StructField("user_id", StringType, false) ::
    StructField("idCard", StringType, true) ::
    StructField("actualFee", DoubleType, true) ::
    StructField("address", MapType(StringType, StringType, true), true) ::
    StructField("createTime", StringType, true) ::
    StructField("endTime", StringType, true) ::
    StructField("invoiceName", StringType, true) ::
    StructField("logistics", MapType(StringType, StringType, true), true) ::
    StructField("orderNumber", StringType, true) ::
    StructField("payTime", StringType, true) ::
    StructField("postFees", DoubleType, true) ::
    StructField("seller", MapType(StringType, StringType, true), true) ::
    StructField("subOrders", ArrayType(MapType(StringType, StringType, true)), true) ::
    StructField("totalQuantity", IntegerType, true) ::
    StructField("tradeNumber", StringType, true) ::
    StructField("tradeStatusName", StringType, true) ::
    StructField("virtualSign", BooleanType, true) ::
    StructField("created_at", StringType, true) ::
    StructField("updated_at", StringType, true) :: Nil
  )

  val schema_compound = StructType(Seq(
    StructField("tid", IntegerType, true),
    StructField("uid", IntegerType, true),
    StructField("st", TimestampType, true),
    StructField("td", StringType, true),
    StructField("trail", ArrayType(StructType(Seq(
      StructField("id", IntegerType, true),
      StructField("ts", LongType, true),
      StructField("lat", DoubleType, true),
      StructField("alt", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("d", StringType, true)))),
    true)))
}
