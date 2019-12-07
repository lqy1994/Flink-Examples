package org.apache.flink.examples.test.udf.table

import com.meituan.meishi.data.lqy.flink.sink.{RetractingSink, StreamITCase}
import com.meituan.meishi.data.lqy.flink.udfExample.WeightedAgg
import org.apache.flink.api.common.time.Time
import org.apache.flink.examples.test.base.AbstractTestBase
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.StreamQueryConfig
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class AggregateITCase extends AbstractTestBase {

  @Test
  def testDistinctAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)
    val qConfig = new StreamQueryConfig
    qConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

    val data = new mutable.MutableList[(Int, Int, String)]
    data.+=((1, 1, "A"))
    data.+=((2, 2, "B"))
    data.+=((2, 2, "B"))
    data.+=((4, 3, "C"))
    data.+=((5, 3, "C"))
    data.+=((4, 3, "C"))
    data.+=((7, 3, "B"))
    data.+=((1, 4, "A"))
    data.+=((9, 4, "D"))
    data.+=((4, 1, "A"))
    data.+=((3, 2, "B"))

    StreamITCase.clear()
    val testAgg = new WeightedAgg
    val t = env.fromCollection(data)
      .toTable(tEnv, 'a, 'b, 'c)
      .groupBy('c)
      .select('c, 'a.count.distinct, 'a.sum.distinct,
        testAgg.distinct('a, 'b), testAgg.distinct('b, 'a), testAgg('a, 'b))

    val results = t.toRetractStream[Row](qConfig)
    val sink = new RetractingSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("A,2,5,1,1,1", "B,3,12,4,2,3", "C,2,9,4,3,4", "D,1,9,9,4,9")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

}
