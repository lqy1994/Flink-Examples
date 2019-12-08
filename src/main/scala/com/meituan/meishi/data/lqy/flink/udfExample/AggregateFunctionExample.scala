package com.meituan.meishi.data.lqy.flink.udfExample

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple, Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.planner
import org.apache.flink.table.planner.JInt
import org.apache.flink.types.Row

import scala.collection.mutable

/**
 * 多行转一行， 聚合函数 agg
 */
object AggregateFunctionExample extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = BatchTableEnvironment.create(env)
  env.setParallelism(1)

  val weiAgg = new WeightedAgg
  tEnv.registerFunction("wAvg", weiAgg)

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

  val table = env.fromCollection(data)
    .toTable(tEnv, 'a, 'b, 'c)

  val aggTable = table
    .groupBy('c)
    .select('c,
      'a.count.distinct,
      'a.sum.distinct,
      weiAgg.distinct('a, 'b),
      weiAgg.distinct('b, 'a),
      weiAgg('a, 'b)
    )

  aggTable.toDataSet[Row].print("agg")

  env.execute("Table Agg Func")
}

class WeightedAvgAcc {
  var sum = 0
  var count = 0
}

class WeightedAgg extends AggregateFunction[Int, WeightedAvgAcc] {
  override def createAccumulator(): WeightedAvgAcc = {
    new WeightedAvgAcc
  }

  override def getValue(accumulator: WeightedAvgAcc): Int = {
    if (accumulator.count == 0) {
      0
    } else {
      accumulator.sum / accumulator.count
    }
  }

  def accumulate(acc: WeightedAvgAcc, iValue: Int, iWeight: Int): Unit = {
    acc.sum += iValue * iWeight
    acc.count += iWeight
  }

  def retract(acc: WeightedAvgAcc, iValue: Int, iWeight: Int): Unit = {
    acc.sum -= iValue * iWeight
    acc.count -= iWeight
  }

  def merge(acc: WeightedAvgAcc, it: java.lang.Iterable[WeightedAvgAcc]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.count += a.count
      acc.sum += a.sum
    }
  }

  def resetAccumulator(acc: WeightedAvgAcc): Unit = {
    acc.count = 0
    acc.sum = 0
  }

//  override def getAccumulatorType: TypeInformation[WeightedAvgAcc] = {
//    new RowTypeInfo[WeightedAvgAcc](classOf[WeightedAvgAcc],
//      Types.LONG, Types.INT)
//  }
//
//  override def getResultType: TypeInformation[JLong] = {
//    Types.LONG()
//  }

}











