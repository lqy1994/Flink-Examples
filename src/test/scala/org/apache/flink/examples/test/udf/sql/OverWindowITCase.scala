package org.apache.flink.examples.test.udf.sql

import com.meituan.meishi.data.lqy.flink.sink.{StreamITCase, StringSink}
import com.meituan.meishi.data.lqy.flink.source.EventTimeSourceFunction
import com.meituan.meishi.data.lqy.flink.udf.LargerThanCount
import org.apache.flink.examples.test.base.AbstractTestBase
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
 * Over Window Agg - SQL
 * over函数可以做到select查询但是不必在group by出现
 */
class OverWindowITCase extends AbstractTestBase {

  @Test
  def testRowTimeBoundedPartitionedRangeOver(): Unit = {
    val data = Seq(
      Left((1500L, (1L, 15, "Hello"))),
      Left((1600L, (1L, 16, "Hello"))),
      Left((1000L, (1L, 1, "Hello"))),
      Left((2000L, (2L, 2, "Hello"))),
      Right(1000L),
      Left((2000L, (2L, 2, "Hello"))),
      Left((2000L, (2L, 3, "Hello"))),
      Left((3000L, (3L, 3, "Hello"))),
      Right(2000L),
      Left((4000L, (4L, 4, "Hello"))),
      Right(3000L),
      Left((5000L, (5L, 5, "Hello"))),
      Right(5000L),
      Left((6000L, (6L, 6, "Hello"))),
      Left((6500L, (6L, 65, "Hello"))),
      Right(7000L),
      Left((9000L, (6L, 9, "Hello"))),
      Left((9500L, (6L, 18, "Hello"))),
      Left((9000L, (6L, 9, "Hello"))),
      Right(10000L),
      Left((10000L, (7L, 7, "Hello World"))),
      Left((11000L, (7L, 17, "Hello World"))),
      Left((11000L, (7L, 77, "Hello World"))),
      Right(12000L),
      Left((14000L, (7L, 18, "Hello World"))),
      Right(14000L),
      Left((15000L, (8L, 8, "Hello World"))),
      Right(17000L),
      Left((20000L, (20L, 20, "Hello World"))),
      Right(19000L)
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear()

    val t1 = env
      .addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("T1", t1)
    tEnv.registerFunction("LARGER_CNT", new LargerThanCount)

    /**
     * Left((1000L, (1L, 1, "Hello"))),
     * Left((1500L, (1L, 15, "Hello"))),
     * Left((1600L, (1L, 16, "Hello"))),
     * ----------------------------------------- > "Hello,1,0,1,1", "Hello,15,0,2,2", "Hello,16,0,3,3",
     * Left((2000L, (2L, 2, "Hello"))),
     * Left((2000L, (2L, 2, "Hello"))),
     * Left((2000L, (2L, 3, "Hello"))),
     * ----------------------------------------- > "Hello,2,0,6,9", "Hello,3,0,6,9", "Hello,2,0,6,9",
     * Left((3000L, (3L, 3, "Hello"))),
     * ----------------------------------------- > "Hello,3,0,4,9",
     * Left((4000L, (4L, 4, "Hello"))),
     * ----------------------------------------- > "Hello,4,0,2,7",
     * Left((5000L, (5L, 5, "Hello"))),
     * ----------------------------------------- > "Hello,5,1,2,9",
     * Left((6000L, (6L, 6, "Hello"))),
     * Left((6500L, (6L, 65, "Hello"))),
     * ----------------------------------------- > "Hello,6,2,2,11", "Hello,65,2,2,12",
     * Left((9000L, (6L, 9, "Hello"))),
     * Left((9000L, (6L, 9, "Hello"))),
     * Left((9500L, (6L, 18, "Hello"))),
     * ----------------------------------------- > "Hello,9,2,2,12", "Hello,9,2,2,12", "Hello,18,3,3,18",
     * Left((10000L, (7L, 7, "Hello World"))),
     * ----------------------------------------- > "Hello World,7,1,1,7",
     * Left((11000L, (7L, 17, "Hello World"))),
     * Left((11000L, (7L, 77, "Hello World"))),
     * ----------------------------------------- > "Hello World,17,3,3,21", "Hello World,77,3,3,21",
     * Left((14000L, (7L, 18, "Hello World"))),
     * ----------------------------------------- > "Hello World,18,1,1,7",
     * Left((15000L, (8L, 8, "Hello World"))),
     * ----------------------------------------- > "Hello World,8,2,2,15",
     * Left((20000L, (20L, 20, "Hello World"))),
     * ----------------------------------------- > "Hello World,20,1,1,20"
     */
    val result = tEnv.sqlQuery(
      """
        |SELECT
        | c, b,
        | LARGER_CNT(a, CAST('4' AS BIGINT)) OVER (PARTITION BY c ORDER BY rowtime RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW),
        | COUNT(a) OVER (PARTITION BY c ORDER BY rowtime RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW),
        | SUM(a) OVER (PARTITION BY c ORDER BY rowtime RANGE BETWEEN INTERVAL '1' SECOND PRECEDING AND CURRENT ROW)
        |FROM T1
        |""".stripMargin).toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()

    val expected = List(
      "Hello,1,0,1,1", "Hello,15,0,2,2", "Hello,16,0,3,3",
      "Hello,2,0,6,9", "Hello,3,0,6,9", "Hello,2,0,6,9",
      "Hello,3,0,4,9",
      "Hello,4,0,2,7",
      "Hello,5,1,2,9",
      "Hello,6,2,2,11", "Hello,65,2,2,12",
      "Hello,9,2,2,12", "Hello,9,2,2,12", "Hello,18,3,3,18",
      "Hello World,7,1,1,7",
      "Hello World,17,3,3,21", "Hello World,77,3,3,21",
      "Hello World,18,1,1,7",
      "Hello World,8,2,2,15",
      "Hello World,20,1,1,20")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver(): Unit = {
    val data = Seq(
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((2L, (2L, 2, "Hello"))),
      Left((1L, (1L, 1, "Hello"))),
      Left((3L, (7L, 7, "Hello World"))),
      Left((1L, (7L, 7, "Hello World"))),
      Left((1L, (7L, 7, "Hello World"))),
      Right(2L),
      Left((3L, (3L, 3, "Hello"))),
      Left((4L, (4L, 4, "Hello"))),
      Left((5L, (5L, 5, "Hello"))),
      Left((6L, (6L, 6, "Hello"))),
      Left((20L, (20L, 20, "Hello World"))),
      Right(6L),
      Left((8L, (8L, 8, "Hello World"))),
      Left((7L, (7L, 7, "Hello World"))),
      Right(20L))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear()

    val t1 = env.addSource[(Long, Int, String)](new EventTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("T1", t1)
    tEnv.registerFunction("LARGER_CNT", new LargerThanCount)
    /**
     * Left((1L, (1L, 1, "Hello"))),
     * Left((1L, (1L, 1, "Hello"))),
     * Left((1L, (1L, 1, "Hello"))),
     * ---------------------------------------------- > win1 > "Hello,1,0,1,1", "Hello,1,0,2,2", "Hello,1,0,3,3",
     * Left((2L, (2L, 2, "Hello"))),
     * Left((2L, (2L, 2, "Hello"))),
     * Left((2L, (2L, 2, "Hello"))),
     * ---------------------------------------------- > win2 > "Hello,2,0,3,4", "Hello,2,0,3,5", "Hello,2,0,3,6",
     * Left((3L, (3L, 3, "Hello"))),
     * Left((4L, (4L, 4, "Hello"))),
     * Left((5L, (5L, 5, "Hello"))),
     * ---------------------------------------------- > win3 > "Hello,3,0,3,7", "Hello,4,0,3,9", "Hello,5,1,3,12",
     * Left((6L, (6L, 6, "Hello"))),
     * ---------------------------------------------- > win4 > "Hello,6,2,3,15",
     * Left((1L, (7L, 7, "Hello World"))),
     * Left((1L, (7L, 7, "Hello World"))),
     * Left((3L, (7L, 7, "Hello World"))),
     * ---------------------------------------------- > win5 > "Hello World,7,1,1,7", "Hello World,7,2,2,14", "Hello World,7,3,3,21",
     * Left((7L, (7L, 7, "Hello World"))),
     * Left((8L, (8L, 8, "Hello World"))),
     * Left((20L, (20L, 20, "Hello World"))),
     * ---------------------------------------------- > win6 > "Hello World,7,3,3,21", "Hello World,8,3,3,22", "Hello World,20,3,3,35"
     */
    val result = tEnv.sqlQuery(
      """
        |SELECT
        | c, a,
        | LARGER_CNT(a, CAST('4' AS BIGINT)) OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
        | COUNT(1)                           OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
        | SUM(a)                             OVER (PARTITION BY c ORDER BY rowtime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        | FROM T1
        |""".stripMargin).toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()

    val expected = List(
      "Hello,1,0,1,1", "Hello,1,0,2,2", "Hello,1,0,3,3",
      "Hello,2,0,3,4", "Hello,2,0,3,5", "Hello,2,0,3,6",
      "Hello,3,0,3,7", "Hello,4,0,3,9", "Hello,5,1,3,12",
      "Hello,6,2,3,15",
      "Hello World,7,1,1,7", "Hello World,7,2,2,14", "Hello World,7,3,3,21",
      "Hello World,7,3,3,21", "Hello World,8,3,3,22", "Hello World,20,3,3,35")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedPartitionedRowsOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    StreamITCase.clear()

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (3, 1L, "Hello")),
      Left(14000003L, (1, 2L, "Hello")),
      Left(14000004L, (1, 3L, "Hello world")),
      Left(14000007L, (3, 2L, "Hello world")),
      Left(14000008L, (2, 2L, "Hello world")),
      Right(14000010L),
      Left(14000012L, (1, 5L, "Hello world")),
      Left(14000021L, (1, 6L, "Hello world")),
      Left(14000023L, (2, 5L, "Hello world")),
      Right(14000020L),
      Left(14000024L, (3, 5L, "Hello world")),
      Left(14000026L, (1, 7L, "Hello world")),
      Left(14000025L, (1, 8L, "Hello world")),
      Left(14000022L, (1, 9L, "Hello world")),
      Right(14000030L))

    val t1 = env.addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)
    tEnv.registerTable("T1", t1)
    tEnv.registerFunction("LARGER_CNT", new LargerThanCount)

    val result = tEnv.sqlQuery(
      """
        |SELECT a, b, c,
        |LARGER_CNT(b, CAST('4' AS BIGINT)) OVER (PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        |SUM(b)                             OVER (PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        |COUNT(b)                           OVER (PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        |AVG(b)                             OVER (PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        |MAX(b)                             OVER (PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        |MIN(b)                             OVER (PARTITION BY a ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |FROM T1
        |""".stripMargin).toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()
    //Bounded RANGE OVER Window 具有相同时间值的所有元素行视为同一计算行，即，具有相同时间值的所有行都是同一个窗口。
    //UNBOUNDED_RANGE 从每个partition的第一行开始
    /**
     * Left(14000002L, (1, 1L, "Hello")),
     * Left(14000002L, (1, 2L, "Hello")),
     * Left(14000002L, (1, 3L, "Hello world")),
     * ---------------------------------------- a - win1
     * Left(14000005L, (1, 1L, "Hi")),
     * ======================================== a - win2
     * Left(14000000L, (2, 1L, "Hello")),
     * ---------------------------------------- b - win1
     * Left(14000003L, (2, 2L, "Hello world")),
     * Left(14000003L, (2, 3L, "Hello world")),
     * ---------------------------------------- b - win2
     * **************************************************
     * Left(14000021L, (1, 4L, "Hello world")),
     * ---------------------------------------- a - win1
     * Left(14000022L, (1, 5L, "Hello world")),
     * Left(14000022L, (1, 6L, "Hello world")),
     * Left(14000022L, (1, 7L, "Hello world")),
     * ======================================== a - win2
     * Left(14000023L, (2, 4L, "Hello world")),
     * Left(14000023L, (2, 5L, "Hello world")),
     * ---------------------------------------- b - win1
     */

    val expected = mutable.MutableList(
      "1,2,Hello,0,2,1,2,2,2",
      "1,3,Hello world,0,5,2,2,3,2",
      "1,1,Hi,0,6,3,2,3,1",
      "2,1,Hello,0,1,1,1,1,1",
      "2,2,Hello world,0,3,2,1,2,1",
      "3,1,Hello,0,1,1,1,1,1",
      "3,2,Hello world,0,3,2,1,2,1",
      "1,5,Hello world,1,11,4,2,5,1",
      "1,6,Hello world,2,17,5,3,6,1",
      "1,9,Hello world,3,26,6,4,9,1",
      "1,8,Hello world,4,34,7,4,9,1",
      "1,7,Hello world,5,41,8,5,9,1",
      "2,5,Hello world,1,8,3,2,5,1",
      "3,5,Hello world,1,8,3,2,5,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedNonPartitionedRangeOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear()

    val data = Seq(
      Left(14000005L, (1, 1L, "Hi")),
      Left(14000000L, (2, 1L, "Hello")),
      Left(14000002L, (1, 1L, "Hello")),
      Left(14000002L, (1, 2L, "Hello")),
      Left(14000002L, (1, 3L, "Hello world")),
      Left(14000003L, (2, 2L, "Hello world")),
      Left(14000003L, (2, 3L, "Hello world")),
      Right(14000020L),
      Left(14000021L, (1, 4L, "Hello world")),
      Left(14000022L, (1, 5L, "Hello world")),
      Left(14000022L, (1, 6L, "Hello world")),
      Left(14000022L, (1, 7L, "Hello world")),
      Left(14000023L, (2, 4L, "Hello world")),
      Left(14000023L, (2, 5L, "Hello world")),
      Right(14000030L))

    val t1 = env
      .addSource(new EventTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    tEnv.registerTable("T1", t1)
    // Bounded ROWS OVER Window 每一行元素都视为新的计算行，即，每一行都是一个新的窗口
    val result = tEnv.sqlQuery(
      """
        |SELECT a, b, c,
        | SUM(b)   OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        | COUNT(b) OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        | AVG(b)   OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        | MAX(b)   OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        | MIN(b)   OVER (ORDER BY rowtime RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |FROM T1
        |""".stripMargin).toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()

    val expected = List(
      "2,1,Hello,1,1,1,1,1",
      "1,1,Hello,7,4,1,3,1",
      "1,2,Hello,7,4,1,3,1",
      "1,3,Hello world,7,4,1,3,1",
      "2,2,Hello world,12,6,2,3,1",
      "2,3,Hello world,12,6,2,3,1",
      "1,1,Hi,13,7,1,3,1",
      "1,4,Hello world,17,8,2,4,1",
      "1,5,Hello world,35,11,3,7,1",
      "1,6,Hello world,35,11,3,7,1",
      "1,7,Hello world,35,11,3,7,1",
      "2,4,Hello world,44,13,3,7,1",
      "2,5,Hello world,44,13,3,7,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
