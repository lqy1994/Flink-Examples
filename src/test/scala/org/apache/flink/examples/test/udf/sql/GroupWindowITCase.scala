package org.apache.flink.examples.test.udf.sql

import java.math.BigDecimal

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.meituan.meishi.data.lqy.flink.sink.{StreamITCase, StringSink}
import com.meituan.meishi.data.lqy.flink.source.EventTimeSourceFunction
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.examples.test.base.AbstractTestBase
import org.apache.flink.examples.test.data.StreamTestData
import org.apache.flink.examples.test.udf.table.GroupWindowITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.formats.json.JsonRowSerializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class GroupWindowITCase extends AbstractTestBase {

  val gson: Gson = new Gson()

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
    val table = stream.toTable(tEnv, 'long1.rowtime, 'int1, 'double1, 'float1, 'bigdec1, 'string1)
    tEnv.registerTable("T", table)
    val windowedTable = tEnv.sqlQuery(
      //HOP(timeCol, slide, size)
      """
        |SELECT
        | string1, COUNT(int1),
        | HOP_START  (long1, INTERVAL '5' SECOND, INTERVAL '10' SECOND),
        | HOP_END    (long1, INTERVAL '5' SECOND, INTERVAL '10' SECOND),
        | HOP_ROWTIME(long1, INTERVAL '5' SECOND, INTERVAL '10' SECOND)
        |FROM T
        |GROUP BY
        | HOP (long1, INTERVAL '5' SECOND, INTERVAL '10' SECOND),
        | string1
        |""".stripMargin)
    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StringSink[Row])
    results.print("")
    env.execute()
  }

  @Test
  def testDistinctAggOnRowTimeTumbleWindow(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    StreamITCase.clear()

    val t = StreamTestData.get5TupleDataStream(env).assignAscendingTimestamps(x => x._2)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'e, 'rowtime.rowtime)
    tEnv.registerTable("MyTable", t)

    val sqlQuery = "SELECT a, " +
      "  SUM(DISTINCT e), " +
      "  MIN(DISTINCT e), " +
      "  COUNT(DISTINCT e)" +
      "FROM MyTable " +
      "GROUP BY a, " +
      "  TUMBLE(rowtime, INTERVAL '5' SECOND) "

    val results = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    results.addSink(new StringSink[Row])
    env.execute()

    val expected = List(
      "1,1,1,1",
      "2,3,1,2",
      "3,5,2,2",
      "4,3,1,2",
      "5,6,1,3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeTumbleWindow(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.testResults = mutable.MutableList()
    StreamITCase.clear()
    env.setParallelism(1)

    val data = List(
      (1000L, "1", "Hello"),
      (2000L, "2", "Hello"),
      (3000L, null.asInstanceOf[String], "Hello"),
      (4000L, "4", "Hello"),
      (5000L, null.asInstanceOf[String], "Hello"),
      (6000L, "6", "Hello"),
      (7000L, "7", "Hello World"),
      (8000L, "8", "Hello World"),
      (20000L, "20", "Hello World"))
    val stream = env.fromCollection(data)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, String, String)](0L))
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", table)

    val sqlQuery = "SELECT c, COUNT(*), COUNT(1), COUNT(b) FROM T1 " +
      "GROUP BY TUMBLE(rowtime, interval '5' SECOND), c"

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()

    val expected = List("Hello World,2,2,2", "Hello World,1,1,1", "Hello,4,4,3", "Hello,2,2,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowRegister(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear()

    val sqlQuery = "SELECT * FROM MyTableRow WHERE c < 3"

    val data = List(
      Row.of("Hello", "Worlds", Int.box(1)),
      Row.of("Hello", "Hiden", Int.box(5)),
      Row.of("Hello again", "Worlds", Int.box(2)))

    implicit val tpe: TypeInformation[Row] = new RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO) // tpe is automatically

    val ds = env.fromCollection(data)

    val t = ds.toTable(tEnv).as('a, 'b, 'c)
    tEnv.registerTable("MyTableRow", t)

    val result = tEnv.sqlQuery(sqlQuery).toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()

    val expected = List("Hello,Worlds,1", "Hello again,Worlds,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testHopStartEndWithHaving(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear()
    env.setParallelism(1)

    val sqlQueryHopStartEndWithHaving =
      """
        |SELECT
        |  c AS k,
        |  COUNT(a) AS v,
        |  HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowStart,
        |  HOP_END  (rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE) AS windowEnd
        |FROM T1
        |GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE), c
        |HAVING
        |  SUM(b) > 1 AND
        |    QUARTER(HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' MINUTE)) = 1
      """.stripMargin

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Right(14000010L),
      Left(8640000000L, (4, 1L, "Hello")), // data for the quarter to validate having filter
      Left(8640000001L, (4, 1L, "Hello")),
      Right(8640000010L)
    )

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)

    val resultHopStartEndWithHaving = tEnv
      .sqlQuery(sqlQueryHopStartEndWithHaving)
      .toAppendStream[Row]
    resultHopStartEndWithHaving.addSink(new StringSink[Row])

    env.execute()

    val expected = List(
      "Hello,2,1970-01-01 03:53:00.0,1970-01-01 03:54:00.0"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAgg(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    val t = env.socketTextStream("localhost", 7777)
      .map(s => {
        val arr = s.split(",")
        (arr(0).trim, arr(1).trim.toLong, arr(2).trim.toInt, new BigDecimal(arr(3).toDouble))
      })
      .assignAscendingTimestamps(x => x._2)
      .toTable(tEnv, 'a, 'b, 'c, 'd, 'rowtime.rowtime)
    tEnv.registerTable("T", t)
    val results = tEnv.sqlQuery(
      """
        |SELECT
        | a AS key,
        | COUNT(1) AS cnt,
        | SUM(d) AS sum_cnt
        |FROM T
        |GROUP BY a
        |""".stripMargin).toRetractStream[Row]
    results.print("")
    results.filter(_._1).map(_._2)
      .writeToSocket("localhost", 8888,
        new JsonRowSerializationSchema.Builder(Types.ROW_NAMED(
          Array[String]("key", "cnt", "sum_cnt"),
          Types.STRING, Types.LONG

          , Types.BIG_DEC)).build())
    env.execute()
  }

  @Test
  def testAgg1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    val t = env.socketTextStream("localhost", 8888)
      .map(s => {
        val t = gson.fromJson(s, new TypeToken[(String, Long, BigDecimal)]() {}.getType)
        t.asInstanceOf[(String, Long, BigDecimal)]
      })
      .assignAscendingTimestamps(x => x._2)
      .toTable(tEnv, 'key, 'cnt, 'sum_cnt, 'rowtime.rowtime)
    tEnv.registerTable("T", t)
    val results = tEnv.sqlQuery(
      """
        |SELECT
        | key,
        | cnt,
        | sum_cnt
        |FROM T
        |""".stripMargin).toRetractStream[Row]
    results.print("")
    env.execute()
  }

}
