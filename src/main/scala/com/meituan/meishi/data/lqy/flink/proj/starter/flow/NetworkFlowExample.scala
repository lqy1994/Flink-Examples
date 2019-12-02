package com.meituan.meishi.data.lqy.flink.proj.starter.flow

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.meituan.meishi.data.lqy.flink.proj.model.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide}
import org.apache.flink.table.catalog.GenericInMemoryCatalog
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NetworkFlowExample {

  val FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val settings = new EnvironmentSettings.Builder().useOldPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    val catalog = new GenericInMemoryCatalog("default")
    tableEnv.registerCatalog("default", catalog)

    val dataStream = env.readTextFile("/Users/liqingyong/Works/Learn/Flink/Proj/github/Flink-Examples/src/main/resources/proj/apache.log")
      .map(data => {
        val arr = data.split(" ")
        val dateTime = LocalDateTime.parse(arr(3), FORMATTER)
        ApacheLogEvent(arr(0).trim, arr(1).trim, Timestamp.valueOf(dateTime).getTime, arr(5).trim, arr(6).trim)
      })
      .filter(log => log.url.startsWith("/blog/"))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      })

    val hotStream = dataStream.keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      //      .allowedLateness(Time.seconds(60)) // 允许60s的延迟数据
      .aggregate(new FlowCountAgg(), new FlowWindowResult()) // 窗口聚合
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
//    hotStream.print("hot")

    val slideTable = tableEnv.fromDataStream(dataStream, 'ip, 'userId, 'method, 'url, 'eventTime.rowtime)
      //      .window(Slide.over(10.minutes).every(5.seconds).on('eventTime).as('win))
      .window(Slide over 10.minutes every 5.seconds on 'eventTime as 'win)
      .groupBy('url, 'win)
      .select('url, 'win.end as 'windowEnd, 'url.count as 'count)

    val urlViewCountStream = tableEnv
      .toRetractStream[(String, Timestamp, Long)](slideTable)
      .map(_._2)
      .map(t => UrlViewCount(t._1, t._2.getTime, t._3))

    val topAgg = new TopAggFunc
    tableEnv.registerFunction("topAgg", topAgg)
    val resTable = slideTable.groupBy('windowEnd)
      .flatAggregate(topAgg('url) as('a, 'b))
      .select('windowEnd, 'a, 'b)

    tableEnv.toRetractStream[Row](resTable).filter(_._1).map(_._2).print("row")

    env.execute("Network Flow")
  }
}

class UvAcc {
  var data: mutable.HashMap[String, Long] = _
//  var smallest: JTuple2[String, Long] = _
  var smallestKey: String = _
  var smallestVal: Long = _
}

class TopAggFunc extends TableAggregateFunction[JTuple2[String, Long], UvAcc] {

  override def createAccumulator(): UvAcc = {
    val acc = new UvAcc
    acc.data = new mutable.HashMap[String, Long]()
    acc.smallestKey = ""
    acc.smallestVal =Long.MaxValue
    acc
  }

  def add(acc: UvAcc, v: String): Unit = {
    var value = acc.data.get(v)
    var cnt: Long = 0L
    if (value == null || value.isEmpty) {
      cnt = 0
    } else {
      cnt = value.get
    }
    acc.data.put(v, cnt + 1)
  }

  def delete(acc: UvAcc, v: String): Unit = {
    if (acc.data.contains(v)) {
      val cnt = acc.data(v) - 1
      if (cnt == 0) {
        acc.data.remove(v)
      } else {
        acc.data.put(v, cnt)
      }
    }
  }

  def accumulate(acc: UvAcc, v: String) {
    if (acc.data.isEmpty) {
      acc.smallestKey = v
      acc.smallestVal = 1
      acc.data.put(v, 1)
    } else if (acc.data.size < 5) {
      add(acc, v)
      val cnt = acc.data(v)
      if (cnt < acc.smallestVal) {
        acc.smallestKey = v
        acc.smallestVal = cnt
      }
    } else {
      val cnt = acc.data.get(v)
      if (cnt != null && cnt.isDefined) {
        if (cnt.get > acc.smallestVal) {
          delete(acc, acc.smallestKey)
          add(acc, v)
          updateSmallest(acc)
        }
      }
    }
  }

  def updateSmallest(acc: UvAcc): Unit = {
    acc.smallestKey = ""
    acc.smallestVal = Long.MaxValue
    val keys = acc.data.keySet.iterator
    while (keys.hasNext) {
      val key = keys.next()
      val value = acc.data(key)
      if (value < acc.smallestVal) {
        acc.smallestKey = key
        acc.smallestVal = value
      }
    }
  }

  def merge(acc: UvAcc, its: Iterable[UvAcc]): Unit = {
    val iter = its.iterator
    while (iter.hasNext) {
      val map = iter.next().data
      val mapIter = map.iterator
      while (mapIter.hasNext) {
        val entry = mapIter.next
        for (_ <- 0 until entry._2.toInt) {
          accumulate(acc, entry._1)
        }
      }
    }
  }

  def emitValue(acc: UvAcc, out: Collector[JTuple2[String, Long]]): Unit = {
    val entries = acc.data.iterator
    while (entries.hasNext) {
      val pair = entries.next()
//      for (_ <- 0 until pair._2.toInt) {
//        out.collect(JTuple2.of(pair._1, 1L))
//      }
      out.collect(JTuple2.of(pair._1, pair._2))
    }
  }

}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount])
  )

  override def processElement(value: UrlViewCount,
                              ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]

    val it = urlState.get().iterator()
    while (it.hasNext) {
      allViews += it.next()
    }

    urlState.clear()

    val sortedUrlViews = allViews
      .sortWith(_.count > _.count) // 从高到低降序排序
      .take(topSize)

    val result: StringBuilder = new StringBuilder
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedUrlViews.indices) {
      val urlview = sortedUrlViews(i)
      result.append("No.").append(i + 1).append(":")
        .append(" URL=").append(urlview.url)
        .append(" 访问量=").append(urlview.count).append("\n")
    }
    result.append("=====================================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}


class FlowCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class FlowWindowResult extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow,
                     input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}