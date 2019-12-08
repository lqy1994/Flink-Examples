package com.meituan.meishi.data.lqy.flink.examples.winFuncs

import com.meituan.meishi.data.lqy.flink.examples.models.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


object SensorReadingExample {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    val stream1 = env.socketTextStream("localhost", 7777)

    val dataStream = stream1.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong,
        dataArr(2).trim.toDouble)
    })
//      .assignAscendingTimestamps(_.timestamp * 1000)
//      .assignTimestampsAndWatermarks(new BoundPeriodAssigner())
//      .assignTimestampsAndWatermarks(new PunctuatedAssigner)
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](
        Time.seconds(1) // 处理乱序数据，并延迟1s
      ) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })


    //统计10s内的最小温度
    val minTempPerWinStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(10))
//      .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.hours(-8)))
      .timeWindow(Time.seconds(15), Time.seconds(5))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    stream1.print("input data")
    minTempPerWinStream.print("min window")

//    dataStream.keyBy(_.id)
//        .process(new MyProcess)

    env.execute(SensorReadingExample.getClass.getSimpleName)
  }
}

class MyProcess extends KeyedProcessFunction[String, SensorReading, String] {
  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    //注册定时器
    ctx.timerService().registerEventTimeTimer(2000)
  }
}

class BoundPeriodAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound: Int = 60000
  var maxTs: Long = Long.MinValue // 保存最大时戳


  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000
  }
}

class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000
  }

}
