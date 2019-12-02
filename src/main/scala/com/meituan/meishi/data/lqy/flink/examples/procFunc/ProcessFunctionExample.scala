package com.meituan.meishi.data.lqy.flink.examples.procFunc

import com.meituan.meishi.data.lqy.flink.examples.models.SensorReading
import com.meituan.meishi.data.lqy.flink.examples.winFuncs.SensorReadingExample
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionExample {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

//    env.setStateBackend(new RocksDBStateBackend())

    val stream1 = env.socketTextStream("localhost", 7777)

    val dataStream = stream1.map(data => {
      val dataArr = data.split(",")
      SensorReading(dataArr(0).trim, dataArr(1).trim.toLong,
        dataArr(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](
          Time.seconds(10000) // 处理乱序数据，并延迟1s
        ) {
          override def extractTimestamp(element: SensorReading): Long = {
            element.timestamp * 1000
          }
        })

    val processStream = dataStream.keyBy(_.id)
      .process(new TempIncrAlert)

    processStream.print("process ")

    env.execute(SensorReadingExample.getClass.getSimpleName)
  }
}

class TempIncrAlert extends KeyedProcessFunction[String, SensorReading, String] {
  // 定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                              out: Collector[String]): Unit = {
    //首先获取上一个温度值
    val preTemp = lastTemp.value()
    //更新温度值
    lastTemp.update(value.temperature)
    val currentTs = currentTimer.value()
    //温度上升且没有设置定时器，则注册定时器
    if (value.temperature > preTemp && currentTimer.value() == 0) {
      val timerTs = ctx.timerService().currentProcessingTime() + 1000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(currentTs)
    } else if (preTemp > value.temperature || preTemp == 0.0) {
      //如果温度下降或第一条数据，删除定时器，并清空状态
      ctx.timerService().deleteEventTimeTimer(currentTs)
      currentTimer.clear()
    }
  }

  //定时器触发 => 输出报警信息
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    out.collect("sensor: " + ctx.getCurrentKey + "温度连续上升")
    currentTimer.clear()
  }
}