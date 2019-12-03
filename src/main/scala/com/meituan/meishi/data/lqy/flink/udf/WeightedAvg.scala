package com.meituan.meishi.data.lqy.flink.udf

import java.lang.{Long => JLong}

import org.apache.flink.table.functions.AggregateFunction

class WeightedAvgAccum {
  var sum: Long = 0L
  var count: Int = 0
}

class WeightedAvg extends AggregateFunction[JLong, WeightedAvgAccum] {

  override def createAccumulator(): WeightedAvgAccum = {
    new WeightedAvgAccum
  }

  override def getValue(accumulator: WeightedAvgAccum): JLong = {
    if (accumulator.count == 0) {
      null
    } else {
      accumulator.sum / accumulator.count
    }
  }

  def accumulate(accumulator: WeightedAvgAccum, value: Long, weight: Int,
                 x: Int, string: String): Unit = {
    accumulator.sum += (value + string.toInt) * weight
    accumulator.count += weight
  }

  def accumulate(accumulator: WeightedAvgAccum, value: Long, weight: Int): Unit = {
    accumulator.sum += value * weight
    accumulator.count += weight
  }

  def accumulate(accumulator: WeightedAvgAccum, value: Int, weight: Int): Unit = {
    accumulator.sum += value * weight
    accumulator.count += weight
  }

}

class WeightedAvgWithMerge extends WeightedAvg {
  def merge(acc: WeightedAvgAccum, it: Iterable[WeightedAvgAccum]): Unit = {
    val iter = it.toIterator
    while (iter.hasNext) {
      val a = iter.next
      acc.count += a.count
      acc.sum += a.sum
    }
  }

  override def toString: String = "myWeightedAvg"
}

class WeightedAvgWithMergeAndReset extends WeightedAvgWithMerge {
  def resetAccumulator(acc: WeightedAvgAccum): Unit = {
    acc.count = 0
    acc.sum = 0L
  }
}

class WeightedAvgWithRetract extends WeightedAvg {
  def retract(accumulator: WeightedAvgAccum, value: Long, weight: Int): Unit = {
    accumulator.sum -= value * weight
    accumulator.count -= weight
  }

  def retract(accumulator: WeightedAvgAccum, value: Int, weight: Int): Unit = {
    accumulator.sum -= value * weight
    accumulator.count -= weight
  }
}