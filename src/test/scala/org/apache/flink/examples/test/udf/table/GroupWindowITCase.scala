package org.apache.flink.examples.test.udf.table

import java.math.BigDecimal

import com.meituan.meishi.data.lqy.flink.sink.{StreamITCase, StringSink}
import com.meituan.meishi.data.lqy.flink.udf.{CountDistinct, WeightedAvg}
import org.apache.flink.examples.test.udf.table.GroupWindowITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Slide, Tumble}
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GroupWindowITCase extends Serializable {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))

  val data2 = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
    (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String]))

  @Test def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.testResults.clear()

    val stream = env.fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](0L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('string), 'int.avg,
         weightAvgFun('long, 'int), weightAvgFun('int, 'int),
        'int.min, 'int.max, 'int.sum,
        'w.start, 'w.end,
        countDistinct('long))

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hello world,1,3,8,3,3,3,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,1",
      "Hello world,1,3,16,3,3,3,3,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,1",
      "Hello,2,2,3,2,2,2,4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2",
      "Hi,1,1,1,1,1,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test def testGroupWindowWithoutKeyInProjection(): Unit = {
    val data = List(
      (1L, 1, "Hi", 1, 1),
      (2L, 2, "Hello", 2, 2),
      (4L, 2, "Hello", 2, 2),
      (8L, 3, "Hello world", 3, 3),
      (16L, 3, "Hello world", 3, 3))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'int2, 'int3, 'proctime.proctime)

    val weightAvgFun = new WeightedAvg
    val countDistinct = new CountDistinct

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'int2, 'int3, 'string)
      .select(weightAvgFun('long, 'int), countDistinct('long))

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StringSink[Row])
    results.print("")
    env.execute()

    val expected = Seq("12,2", "8,1", "2,1", "3,2", "1,1")
//    assertEquals(expected.sorted, testResults.sorted)
  }

}

object GroupWindowITCase {

  class TimestampAndWatermarkWithOffset[T <: Product](Offset: Long)
    extends AssignerWithPunctuatedWatermarks[T] {

    override def checkAndGetNextWatermark(lastElement: T,
                                          extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - Offset)
    }

    override def extractTimestamp(element: T,
                                  previousElementTimestamp: Long): Long = {
      element.productElement(0).asInstanceOf[Number].longValue()
    }
  }

}
