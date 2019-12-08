package org.apache.flink.examples.test.udf.table

import com.meituan.meishi.data.lqy.flink.sink.{RetractingSink, StreamITCase}
import com.meituan.meishi.data.lqy.flink.udf.{TopN, TopNAccum}
import org.apache.flink.examples.test.base.AbstractTestBase
import org.apache.flink.examples.test.data.StreamTestData
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

class TableAggregateITCase extends AbstractTestBase {

  @Test
  def testGroupByFlatAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    StreamITCase.clear()

    val top3 = new TopN(3)
    val source = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source.groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as('category, 'v1, 'v2)

    val results = resultTable.toRetractStream[Row]
    results.addSink(new RetractingSink)
    env.execute()

    val expected = List(
      "1,1,1",
      "2,2,2",
      "2,3,3",
      "3,4,4",
      "3,5,5",
      "3,6,6",
      "4,10,10",
      "4,9,9",
      "4,8,8",
      "5,15,15",
      "5,14,14",
      "5,13,13",
      "6,21,21",
      "6,20,20",
      "6,19,19"
    ).sorted
    assertEquals(expected, StreamITCase.retractedResults.sorted)

  }

}
