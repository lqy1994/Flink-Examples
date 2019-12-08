package com.meituan.meishi.data.lqy.flink.udfExample

import java.lang.{Integer => JInt, Iterable => JIterable}
import java.util

import com.meituan.meishi.data.lqy.flink.sink.{RetractingSink, StreamITCase}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object TableAggregateFunctionExample extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tEnv = StreamTableEnvironment.create(env)
  env.setParallelism(1)

  val data = new mutable.MutableList[(Int, Long, String)]
  data.+=((1, 1L, "Hi"))
  data.+=((2, 2L, "Hello"))
  data.+=((3, 2L, "Hello world"))
  data.+=((4, 3L, "Hello world, how are you?"))
  data.+=((5, 3L, "I am fine."))
  data.+=((6, 3L, "Luke Skywalker"))
  data.+=((7, 4L, "Comment#1"))
  data.+=((8, 4L, "Comment#2"))
  data.+=((9, 4L, "Comment#3"))
  data.+=((10, 4L, "Comment#4"))
  data.+=((11, 5L, "Comment#5"))
  data.+=((12, 5L, "Comment#6"))
  data.+=((13, 5L, "Comment#7"))
  data.+=((14, 5L, "Comment#8"))
  data.+=((15, 5L, "Comment#9"))
  data.+=((16, 6L, "Comment#10"))
  data.+=((17, 6L, "Comment#11"))
  data.+=((18, 6L, "Comment#12"))
  data.+=((19, 6L, "Comment#13"))
  data.+=((20, 6L, "Comment#14"))
  data.+=((21, 6L, "Comment#15"))

  StreamITCase.clear()
  val top3 = new Top3AggFunc
  val table = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
  val res = table.groupBy('b)
    .flatAggregate(top3('a) as('f1, 'f2))
    .select('b, 'f1, 'f2)
    .as('category, 'v1, 'v2)

  val sink = new RetractingSink
  res.toRetractStream[Row].addSink(sink)
  println(StreamITCase.retractedResults)

  env.execute("Table Agg Func")
}

class Top3Accum {
  var data: util.HashMap[JInt, JInt] = _
  var size: JInt = _
  var smallest: JInt = _
}

class Top3AggFunc extends TableAggregateFunction[JTuple2[JInt, JInt], Top3Accum] {
  override def createAccumulator(): Top3Accum = {
    val acc = new Top3Accum
    acc.data = new util.HashMap[JInt, JInt]()
    acc.size = 0
    acc.smallest = Integer.MAX_VALUE
    acc
  }

  def add(acc: Top3Accum, v: Int): Unit = {
    var cnt = acc.data.get(v)
    acc.size += 1
    if (cnt == null) {
      cnt = 0
    }
    acc.data.put(v, cnt + 1)
  }

  def delete(acc: Top3Accum, v: Int): Unit = {
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

  def updateSmallest(acc: Top3Accum): Unit = {
    acc.smallest = Integer.MAX_VALUE
    val keys = acc.data.keySet().iterator()
    while (keys.hasNext) {
      val key = keys.next()
      if (key < acc.smallest) {
        acc.smallest = key
      }
    }
  }

  def accumulate(acc: Top3Accum, v: Int) {
    if (acc.size == 0) {
      acc.size = 1
      acc.smallest = v
      acc.data.put(v, 1)
    } else if (acc.size < 3) {
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

  def merge(acc: Top3Accum, its: JIterable[Top3Accum]): Unit = {
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

  def emitValue(acc: Top3Accum, out: Collector[JTuple2[JInt, JInt]]): Unit = {
    val entries = acc.data.entrySet().iterator()
    while (entries.hasNext) {
      val pair = entries.next()
      for (_ <- 0 until pair.getValue) {
        out.collect(JTuple2.of(pair.getKey, pair.getKey))
      }
    }
  }

}