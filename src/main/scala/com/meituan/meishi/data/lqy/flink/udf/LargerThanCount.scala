package com.meituan.meishi.data.lqy.flink.udf

import org.apache.flink.api.java.tuple._
import org.apache.flink.table.functions.AggregateFunction

class LargerThanCount extends AggregateFunction[Long, Tuple1[Long]] {

  override def createAccumulator(): Tuple1[Long] = Tuple1.of(0L)

  override def getValue(accumulator: Tuple1[Long]): Long = {
    accumulator.f0
  }

  def accumulate(acc: Tuple1[Long], a: Long, b: Long): Unit = {
    if (a > b) acc.f0 += 1
  }

  def retract(acc: Tuple1[Long], a: Long, b: Long): Unit = {
    if (a > b) acc.f0 -= 1
  }

}
