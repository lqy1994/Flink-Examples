package com.meituan.meishi.data.lqy.flink.udf

import java.lang.{Iterable => JIterable}
import java.util

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector


class TopNAccum {
  var data: util.HashMap[Integer, Integer] = _
  var size: Integer = _
  var smallest: Integer = _
}

class TopN(size: Int) extends TableAggregateFunction[JTuple2[Integer, Integer], TopNAccum] {
  override def createAccumulator(): TopNAccum = {
    val acc = new TopNAccum
    acc.data = new util.HashMap[Integer, Integer]()
    acc.size = 0
    acc.smallest = Integer.MAX_VALUE
    acc
  }

  def add(acc: TopNAccum, v: Int): Unit = {
    var cnt = acc.data.get(v)
    acc.size += 1
    if (cnt == null) {
      cnt = 0
    }
    acc.data.put(v, cnt + 1)
  }

  def delete(acc: TopNAccum, v: Int): Unit = {
    if (acc.data.containsKey(v)) {
      acc.size -= 1
      val cnt = acc.data.get(v) - 1
      if (cnt == 0) {
        acc.data.remove(v)
      } else {
        acc.data.put(v, cnt)
      }
    }
  }

  def updateSmallest(acc: TopNAccum): Unit = {
    acc.smallest = Integer.MAX_VALUE
    val keys = acc.data.keySet().iterator()
    while (keys.hasNext) {
      val key = keys.next()
      if (key < acc.smallest) {
        acc.smallest = key
      }
    }
  }

  def accumulate(acc: TopNAccum, v: Int): Unit = {
    if (acc.size == 0) {
      acc.size = 1
      acc.smallest = v
      acc.data.put(v, 1)
    } else if (acc.size < size) {
      add(acc, v)
      if (v < acc.smallest) {
        acc.smallest = v
      }
    } else if (v > acc.smallest) {
      delete(acc, acc.smallest)
      add(acc, v)
      updateSmallest(acc)
    }
  }

  def merge(acc: TopNAccum, its: JIterable[TopNAccum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val map = iter.next().data
      val mapIter = map.entrySet().iterator()
      while (mapIter.hasNext) {
        val entry = mapIter.next()
        for (_ <- 0 until entry.getValue) {
          accumulate(acc, entry.getKey)
        }
      }
    }
  }

  def emitValue(acc: TopNAccum, out: Collector[JTuple2[Integer, Integer]]): Unit = {
    val entries = acc.data.entrySet().iterator()
    while (entries.hasNext) {
      val pair = entries.next()
      for (_ <- 0 until pair.getValue) {
        out.collect(JTuple2.of(pair.getKey, pair.getKey))
      }
    }
  }

}
