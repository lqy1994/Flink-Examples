package com.meituan.meishi.data.lqy.flink.proj.starter.hotItem.kafka

import java.sql.Timestamp
import java.util.Properties

import com.meituan.meishi.data.lqy.flink.proj.model.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object KafkaHotItems {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems",
      new SimpleStringSchema(), properties))
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong,
          arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //每个商品的计数值
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      //      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResult()) // 窗口聚合
      .keyBy(_.windowEnd) // 窗口分组
      .process(new TopNHotItems(3))

    //    dataStream.print()
    processedStream.print("process")

    env.execute("HotItems")
  }
}

/**
 * 自定义预聚合函数 -- Count计数
 */
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
 * 自定义预聚合函数 -- 计算平均数
 */
class AvgAgg extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def createAccumulator(): (Long, Int) = (0, 0)

  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) =
    (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def getResult(accumulator: (Long, Int)): Double =
    accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) =
    (a._1 + b._1, a._2 + b._2)
}

/**
 * 自定义窗口函数，输出 ItemViewCount
 */
class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow,
                     input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//class WindowResult extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
//  override def apply(key: Tuple, window: TimeWindow,
//                     input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//    val itemId : Long = key.asInstanceOf[Tuple1[Long]].f0
//
////    out.collect(ItemViewCount(key, , window.getEnd, ))
//  }
//}

class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(
      new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount])
    )
  }

  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    // 将每条数据存入状态列表
    itemState.add(value)
    //注册一个定时器，延迟1ms
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //定时器触发时，对所有数据排序并输出结果
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    //将state所有数据取出
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()

    itemState.get().forEach(item => allItems += item)

    //按照点击量count大小排序（降序）并取前N个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //清空状态，释放内存空间
    itemState.clear()

    //将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 输出每一个商品信息
    for (i <- sortedItems.indices) { // 所有下标
      val current = sortedItems(i)
      result.append("No.").append(i + 1).append(":")
        .append(" 商品ID=").append(current.itemId)
        .append("\t浏览量=").append(current.count)
        .append("\n")
    }

    result.append("==============================")
    //控制输出频率
    Thread.sleep(1000)
    //必须collect才能输出
    out.collect(result.toString())
  }

}
