package org.apache.flink.examples.test.udf.table

import com.meituan.meishi.data.lqy.flink.sink.{StreamITCase, StringSink}
import com.meituan.meishi.data.lqy.flink.source.RowTimeSourceFunction
import com.meituan.meishi.data.lqy.flink.udf.JavaUserDefinedScalarFunctions.JavaFunc0
import com.meituan.meishi.data.lqy.flink.udf.{CountDistinct, CountDistinctWithRetractAndReset, WeightedAvg}
import org.apache.flink.examples.test.base.AbstractTestBase
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Over
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
 * Over window 聚合函数Agg - Table
 */
class OverWindowITCase extends AbstractTestBase {

  @Test
  def testOverWindowWitchConstant(): Unit = {
    val data = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (3L, 3, "Hello"),
      (4L, 4, "Hello"),
      (5L, 5, "Hello"),
      (6L, 6, "Hello"),
      (7L, 7, "Hello World"),
      (8L, 8, "Hello World"),
      (8L, 8, "Hello World"),
      (20L, 20, "Hello World"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'a, 'b, 'c, 'proctime.proctime)
    val weightAgg = new WeightedAvg

    val winTable = table
      .window(
        Over partitionBy 'c orderBy 'proctime preceding UNBOUNDED_ROW as 'w)
      .select('c, weightAgg('a, 42, 'b, "2") over 'w as 'wAvg)

    val results = winTable.toAppendStream[Row]
    results.addSink(new StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hello World,12", "Hello World,9", "Hello World,9", "Hello World,9", "Hello,3",
      "Hello,3", "Hello,4", "Hello,4", "Hello,5", "Hello,5")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeUnBoundedPartitionedRangeOver(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    StreamITCase.clear()

    val data: Seq[Either[(Long, (Int, Long, String)), Long]] = Seq(
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
      Right(14000030L)
    )
    /**
     * PART a
     * Left(14000002L, (1, 1L, "Hello")),
     * Left(14000002L, (1, 2L, "Hello")),
     * Left(14000002L, (1, 3L, "Hello world")),
     * ---------------------------------------- winA1
     * Left(14000005L, (1, 1L, "Hi")),
     * ======================================== winA2
     * PART b :
     * Left(14000000L, (2, 1L, "Hello")),
     * ---------------------------------------- winB1
     * Left(14000003L, (2, 2L, "Hello world")),
     * Left(14000003L, (2, 3L, "Hello world")),
     * ---------------------------------------- winB2
     * **************************************************
     * Left(14000021L, (1, 4L, "Hello world")), a-win1
     * ----------------------------------------
     * Left(14000022L, (1, 5L, "Hello world")), a-win2
     * Left(14000022L, (1, 6L, "Hello world")),
     * Left(14000022L, (1, 7L, "Hello world")),
     * ========================================
     * Left(14000023L, (2, 4L, "Hello world")), b-win1
     * Left(14000023L, (2, 5L, "Hello world")),
     *
     */
    val table = env.addSource(new RowTimeSourceFunction[(Int, Long, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    val countFun = new CountAggFunction
    val weightFun = new WeightedAvg
    val plusOne = new JavaFunc0
    val countDist = new CountDistinct

    val windowTable = table
      //Bounded RANGE OVER Window 具有相同时间值的所有元素行视为同一计算行，即，具有相同时间值的所有行都是同一个窗口。
      //UNBOUNDED_RANGE 从每个partition的第一行开始
      .window(Over partitionBy 'a orderBy 'rowtime preceding UNBOUNDED_RANGE following CURRENT_RANGE as 'w)
    val wTable = windowTable.select('a, 'b, 'c, 'rowtime)
    wTable.toAppendStream[Row].print("window")

    val resTable = windowTable
      .select(
        'a, 'b, 'c,
        'b.sum over 'w,
        "SUM:".toExpr + ('b.sum over ('w)),
        countFun('b) over 'w,
        (countFun('b) over 'w) + 1,
        plusOne((countFun('b) over 'w)),
        array('b.avg over 'w, 'b.max over 'w),
        'b.avg over 'w,
        'b.max over 'w,
        'b.min over 'w,
        ('b.min over 'w).abs(),
        weightFun('b, 'a) over 'w,
        countDist('c) over 'w as 'CountDist
      )

    val result = resTable.toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hello,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,2,Hello,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,3,Hello world,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,1,Hi,7,SUM:7,4,5,5,[1, 3],1,3,1,1,1,3",
      "2,1,Hello,1,SUM:1,1,2,2,[1, 1],1,1,1,1,1,1",
      "2,2,Hello world,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "2,3,Hello world,6,SUM:6,3,4,4,[2, 3],2,3,1,1,2,2",
      "1,4,Hello world,11,SUM:11,5,6,6,[2, 4],2,4,1,1,2,3",
      "1,5,Hello world,29,SUM:29,8,9,9,[3, 7],3,7,1,1,3,3",
      "1,6,Hello world,29,SUM:29,8,9,9,[3, 7],3,7,1,1,3,3",
      "1,7,Hello world,29,SUM:29,8,9,9,[3, 7],3,7,1,1,3,3",
      "2,4,Hello world,15,SUM:15,5,6,6,[3, 5],3,5,1,1,3,2",
      "2,5,Hello world,15,SUM:15,5,6,6,[3, 5],3,5,1,1,3,2"
    )

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testRowTimeBoundedPartitionRowOver(): Unit = {
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
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStateBackend(getStateBackend)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear()

    val countDist = new CountDistinctWithRetractAndReset
    val table = env.addSource[(Long, Int, String)](
      new RowTimeSourceFunction[(Long, Int, String)](data))
      .toTable(tEnv, 'a, 'b, 'c, 'rowtime.rowtime)

    val windowedTable = table
      // Bounded ROWS OVER Window 每一行元素都视为新的计算行，即，每一行都是一个新的窗口
      .window(Over partitionBy 'c orderBy 'rowtime preceding 2.rows following CURRENT_ROW as 'w)
      .select('c, 'a, 'a.count over 'w, 'a.sum over 'w, countDist('a) over 'w)

    val result = windowedTable.toAppendStream[Row]
    result.addSink(new StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "Hello,1,1,1,1", "Hello,1,2,2,1", "Hello,1,3,3,1",
      "Hello,2,3,4,2", "Hello,2,3,5,2", "Hello,2,3,6,1",
      "Hello,3,3,7,2", "Hello,4,3,9,3", "Hello,5,3,12,3",
      "Hello,6,3,15,3",
      "Hello World,7,1,7,1", "Hello World,7,2,14,1", "Hello World,7,3,21,1",
      "Hello World,7,3,21,1", "Hello World,8,3,22,2", "Hello World,20,3,35,3")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }


}

