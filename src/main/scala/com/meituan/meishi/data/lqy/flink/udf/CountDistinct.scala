package com.meituan.meishi.data.lqy.flink.udf

import java.lang.{Long => JLong}

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala._
import org.apache.flink.table.api.dataview.MapView
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction

class CountDistinctAccum {
  var map: MapView[String, Integer] = _
  var count: JLong = _
}

class CountDistinct extends AggregateFunction[JLong, CountDistinctAccum] {

  override def createAccumulator(): CountDistinctAccum = {
    val acc = new CountDistinctAccum
    acc.map = new MapView(Types.STRING, Types.INT)
    acc.count = 0L
    acc
  }

  def accumulate(acc: CountDistinctAccum, id: String): Unit = {
    try {
      var cnt: Integer = acc.map.get(id)
      if (cnt != null) {
        cnt += 1
        acc.map.put(id, cnt)
      } else {
        acc.map.put(id, 1)
        acc.count += 1
      }
    } catch {
      case e: Exception => println(e)
    }
  }

  def accumulate(acc: CountDistinctAccum, id: JLong): Unit = {
    try {
      var cnt: Integer = acc.map.get(String.valueOf(id))
      if (cnt != null) {
        cnt += 1
        acc.map.put(String.valueOf(id), cnt)
      } else {
        acc.map.put(String.valueOf(id), 1)
        acc.count += 1
      }
    } catch {
      case e: Exception => println(e)
    }
  }

  override def getValue(accumulator: CountDistinctAccum): JLong =
    accumulator.count

}

/**
 * CountDistinct aggregate with merge.
 */
class CountDistinctWithMerge extends CountDistinct {

  def merge(acc: CountDistinctAccum, it: Iterable[CountDistinctAccum]): Unit = {
    val iter: Iterator[CountDistinctAccum] = it.toIterator
    while (iter.hasNext) {
      val mergeAcc: CountDistinctAccum = iter.next
      acc.count += mergeAcc.count

      try {
        val itrMap = mergeAcc.map.iterator()
        while (itrMap.hasNext) {
          val entry = itrMap.next
          val key: String = entry.getKey
          val cnt: Integer = entry.getValue
          if (acc.map.contains(key)) {
            acc.map.put(key, acc.map.get(key) + cnt)
          } else {
            acc.map.put(key, cnt)
          }
        }
      } catch {
        case e: Exception => println(e)
      }
    }

  }

}

/**
 * CountDistinct aggregate with retract.
 */
class CountDistinctWithRetractAndReset extends CountDistinct {

  def retract(acc: CountDistinctAccum, id: JLong): Unit = {
    try {
      var cnt: Integer = acc.map.get(String.valueOf(id))
      if (cnt != null) {
        cnt -= 1
        if (cnt <= 0) {
          acc.map.remove(String.valueOf(id))
          acc.count -= 1
        } else {
          acc.map.put(String.valueOf(id), cnt)
        }
      }
    } catch {
      case e: Exception => println(e)
    }
  }

}
