package org.apache.flink.examples.test.udf.table

import java.math.BigDecimal

import com.meituan.meishi.data.lqy.flink.sink.{StreamITCase, StringSink}
import com.meituan.meishi.data.lqy.flink.udf.{CountDistinct, WeightedAvg}
import org.apache.flink.examples.test.udf.table.GroupWindowITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Slide, Tumble}
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class GroupWindowITCase extends Serializable {

  // ----------------------------------------------------------------------------------------------
  // Tumble windows
  // ----------------------------------------------------------------------------------------------
  @Test def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.testResults.clear()

    val data = List(
      (1L, 1, "Hi"),
      (2L, 2, "Hello"),
      (4L, 2, "Hello"),
      (8L, 3, "Hello world"),
      (16L, 3, "Hello world"))
    /**
     * long,int,string
     * --------------------------
     * (1L, 1, "Hi"),
     * (2L, 2, "Hello"),                 Hi,   1,1,1,1,1,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1
     * (4L, 2, "Hello"),                 Hello,2,2,3,2,2,2,4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2
     * -------------------------- win1 >
     * (8L, 3, "Hello world"),
     * -------------------------- win2 > Hello world,1,3,8,3,3,3,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,1
     * (16L, 3, "Hello world"))
     * -------------------------- win3 > Hello world,1,3,16,3,3,3,3,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,1
     */
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
    results.print()
    env.execute()

    val expected = Seq(
      "Hello world,1,3,8,3,3,3,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01,1",
      "Hello world,1,3,16,3,3,3,3,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02,1",
      "Hello,2,2,3,2,2,2,4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,2",
      "Hi,1,1,1,1,1,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------
  @Test def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.testResults = mutable.MutableList()
    val data2 = List(
      //     long,int,double,float,bigdec,            string
      (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
      (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
      (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
      (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
      (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
      (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
      (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
      (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String]))
    /**
     * ------------------------------------------------ => w1.st: -2ms
     * ------------------------------------------------ => w2.st: +0ms
     * (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
     * ------------------------------------------------ 1ms
     * (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
     * ------------------------------------------------ => w3.st: +2ms ===> 2,-2ms,+3ms,+2ms
     * ------------------------------------------------ => w1.en: +3ms ===>
     * (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
     * (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
     * ------------------------------------------------ => w4.st: +4ms ===> 4,+0ms,+5ms,+4ms
     * ------------------------------------------------ => w2.en: +5ms
     * ------------------------------------------------ => w5.st: +6ms ===> 3,+2ms,+7ms,+6ms
     * ------------------------------------------------ => w3.en: +7ms
     * (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
     * (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
     * ------------------------------------------------ => w6.st: +8ms ===> 3,+4ms,+9ms,+8ms
     * ------------------------------------------------ => w4.en: +9ms
     * ------------------------------------------------ 10ms           ===> 2,+6ms,11ms,10ms
     * ------------------------------------------------ => w5.en: +11ms
     * ------------------------------------------------ => w7.st: +12ms===> 1,+8ms,13ms,12ms
     * ------------------------------------------------ => w6.en: +13ms
     * ------------------------------------------------ => w8.st: +14ms
     * ------------------------------------------------ 15ms
     * (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
     * ------------------------------------------------ => w9.st: +16ms===> 1,12ms,17ms,+16ms
     * ------------------------------------------------ => w7.en: +17ms
     * ------------------------------------------------ 18ms           ===> 1,14ms,19ms,18ms
     * ------------------------------------------------ => w8.en: +19ms
     * ------------------------------------------------ 20ms           ===> 1,16ms,21ms,20ms
     * ------------------------------------------------ => w9.en: +21ms
     * ------------------------------------------------ => w10.st:+28ms
     * (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String]))
     * ------------------------------------------------ => 32ms        ===> 1,28ms,33ms,32ms
     * ------------------------------------------------ => w10.en:+33ms
     */
    val stream =
    //      env.socketTextStream("localhost", 9672)
    //      .map(line => {
    //        val split = line.split(",")
    //        (split(0).trim.toLong, split(1).trim.toInt, split(2).trim.toDouble,
    //          split(3).trim.toFloat, new BigDecimal(split(4).trim.toDouble), split(5).trim)
    //      })
      env.fromCollection(data2)
        .assignTimestampsAndWatermarks(
          new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)
    val windowedTable = table
      .window(Slide over 5.milli every 2.milli on 'long as 'w)
      .groupBy('w)
      .select('int.count, 'w.start, 'w.end, 'w.rowtime)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StringSink[Row])
    windowedTable.toRetractStream[Row].print("")
    env.execute()

    val expected = Seq(
      "1,1970-01-01 00:00:00.008,1970-01-01 00:00:00.013,1970-01-01 00:00:00.012",
      "1,1970-01-01 00:00:00.012,1970-01-01 00:00:00.017,1970-01-01 00:00:00.016",
      "1,1970-01-01 00:00:00.014,1970-01-01 00:00:00.019,1970-01-01 00:00:00.018",
      "1,1970-01-01 00:00:00.016,1970-01-01 00:00:00.021,1970-01-01 00:00:00.02",
      "2,1969-12-31 23:59:59.998,1970-01-01 00:00:00.003,1970-01-01 00:00:00.002",
      "2,1970-01-01 00:00:00.006,1970-01-01 00:00:00.011,1970-01-01 00:00:00.01",
      "3,1970-01-01 00:00:00.002,1970-01-01 00:00:00.007,1970-01-01 00:00:00.006",
      "3,1970-01-01 00:00:00.004,1970-01-01 00:00:00.009,1970-01-01 00:00:00.008",
      "4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005,1970-01-01 00:00:00.004",
      "1,1970-01-01 00:00:00.028,1970-01-01 00:00:00.033,1970-01-01 00:00:00.032",
      "1,1970-01-01 00:00:00.03,1970-01-01 00:00:00.035,1970-01-01 00:00:00.034",
      "1,1970-01-01 00:00:00.032,1970-01-01 00:00:00.037,1970-01-01 00:00:00.036")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSlidingGroupWindowOverTimeOverlappingFullPane(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.testResults = mutable.MutableList()

    val data2 = List(
      (1000L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
      (2000L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
      (3000L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),

      (4000L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
      (7000L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
      (8000L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),

      (16000L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),

      (32000L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String]))
    val stream =
//      env.socketTextStream("localhost", 9672)
//        .map(line => {
//          val split = line.split(",")
//          (split(0).trim.toLong, split(1).trim.toInt, split(2).trim.toDouble,
//            split(3).trim.toFloat, new BigDecimal(split(4).trim.toDouble), split(5).trim)
//        })
      env.fromCollection(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val windowedTable = table
      .window(Slide over 10.second every 5.second on 'long as 'w)
      .groupBy('w, 'string)
      .select('string, 'int.count, 'w.start, 'w.end, 'w.rowtime)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StringSink[Row])
    results.print("")
    env.execute()
//    ---------------------------------------------------- -5  w1:st
//
//    ----------------------------------------------------  0  w2:st
//    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),  -- w1,w2
//    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),-- w1,w2
//    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),-- w1,w2
//    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),-- w1,w2
//    ----------------------------------------------------  5  5 w3:st
//    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),-- w2
//    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),-- w2
//    ----------------------------------------------------  10 w2:en w4:st
//
//    ----------------------------------------------------  15 w5:st w3:en
//    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"), --w4
//    ----------------------------------------------------  20       w4:en
//
//    ----------------------------------------------------  25 w5:en w6:st
//
//    ----------------------------------------------------  30 w7:st
//    (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String])) --w6
//    ----------------------------------------------------  35       w6:en
//
//    ----------------------------------------------------  40 w7:en
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
