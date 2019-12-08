package com.meituan.meishi.data.lqy.flink.udf

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.dataview.MapView

import scala.collection.JavaConverters

object MapViewExample extends App {
  val map: MapView[String, Integer] = new MapView(Types.STRING, Types.INT)

  map.putAll(JavaConverters.mapAsJavaMap(
    Map(
      "a" -> Integer.valueOf(1),
      "b" -> Integer.valueOf(2),
      "c" -> Integer.valueOf(3),
      "a" -> Integer.valueOf(5)
    )))

  println(map.map)
}
