package org.apache.flink.examples.test

import org.apache.flink.types.Row
import java.lang.{Long => JLong}

import scala.collection.mutable


class SeqHolder {

  val customerData = new mutable.MutableList[(String, String, String)]
  customerData.+=(("c_001", "Kevin", "from JinLin"))
  customerData.+=(("c_002", "Sunny", "from JinLin"))
  customerData.+=(("c_003", "JinCheng", "from HeBei"))


  // 订单表数据
  val orderData = new mutable.MutableList[(String, String, String, String)]
  orderData.+=(("o_001", "c_002", "2018-11-05 10:01:01", "iphone"))
  orderData.+=(("o_002", "c_001", "2018-11-05 10:01:55", "ipad"))
  orderData.+=(("o_003", "c_001", "2018-11-05 10:03:44", "flink book"))

  //商品销售表数据
  val itemData = Seq(
    Left((1510365660000L, (1510365660000L, 20, "ITEM001", "Electronic"))),
    Right(1510365660000L),
    Left((1510365720000L, (1510365720000L, 50, "ITEM002", "Electronic"))),
    Right(1510365720000L),
    Left((1510365780000L, (1510365780000L, 30, "ITEM003", "Electronic"))),
    Left((1510365780000L, (1510365780000L, 60, "ITEM004", "Electronic"))),
    Right(1510365780000L),
    Left((1510365900000L, (1510365900000L, 40, "ITEM005", "Electronic"))),
    Right(1510365900000L),
    Left((1510365960000L, (1510365960000L, 20, "ITEM006", "Electronic"))),
    Right(1510365960000L),
    Left((1510366020000L, (1510366020000L, 70, "ITEM007", "Electronic"))),
    Right(1510366020000L),
    Left((1510366080000L, (1510366080000L, 20, "ITEM008", "Clothes"))),
    Right(1510366080000L)
  )

  //页面访问表数据
  val pageAccessData = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", "U0010"))),
    Right(1510365660000L),
    Left((1510365660000L, (1510365660000L, "BeiJing", "U1001"))),
    Right(1510365660000L),
    Left((1510366200000L, (1510366200000L, "BeiJing", "U2032"))),
    Right(1510366200000L),
    Left((1510366260000L, (1510366260000L, "BeiJing", "U1100"))),
    Right(1510366260000L),
    Left((1510373400000L, (1510373400000L, "ShangHai", "U0011"))),
    Right(1510373400000L)
  )

  // 页面访问量表数据2
  val pageAccessCountData = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", 100))),
    Right(1510365660000L),
    Left((1510365660000L, (1510365660000L, "BeiJing", 86))),
    Right(1510365660000L),
    Left((1510365960000L, (1510365960000L, "BeiJing", 210))),
    Right(1510366200000L),
    Left((1510366200000L, (1510366200000L, "BeiJing", 33))),
    Right(1510366200000L),
    Left((1510373400000L, (1510373400000L, "ShangHai", 129))),
    Right(1510373400000L))

  // 页面访问表数据3
  val pageAccessSessionData = Seq(
    Left((1510365660000L, (1510365660000L, "ShangHai", "U0011"))),
    Right(1510365660000L),
    Left((1510365720000L, (1510365720000L, "ShangHai", "U0012"))),
    Right(1510365720000L),
    Left((1510365720000L, (1510365720000L, "ShangHai", "U0013"))),
    Right(1510365720000L),
    Left((1510365900000L, (1510365900000L, "ShangHai", "U0015"))),
    Right(1510365900000L),
    Left((1510366200000L, (1510366200000L, "ShangHai", "U0011"))),
    Right(1510366200000L),
    Left((1510366200000L, (1510366200000L, "BeiJing", "U2010"))),
    Right(1510366200000L),
    Left((1510366260000L, (1510366260000L, "ShangHai", "U0011"))),
    Right(1510366260000L),
    Left((1510373760000L, (1510373760000L, "ShangHai", "U0410"))),
    Right(1510373760000L)
  )

  val data = Seq(
    Left(1510365660000L, Row.of(new JLong(1510365660000L), "ShangHai", "U0010")),
    Right(1510365660000L),
    Left(1510365660000L, Row.of(new JLong(1510365660000L), "BeiJing", "U1001")),
    Right(1510365660000L),
    Left(1510366200000L, Row.of(new JLong(1510366200000L), "BeiJing", "U2032")),
    Right(1510366200000L),
    Left(1510366260000L, Row.of(new JLong(1510366260000L), "BeiJing", "U1100")),
    Right(1510366260000L),
    Left(1510373400000L, Row.of(new JLong(1510373400000L), "ShangHai", "U0011")),
    Right(1510373400000L)
  )

}
