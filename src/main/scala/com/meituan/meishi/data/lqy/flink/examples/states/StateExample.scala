package com.meituan.meishi.data.lqy.flink.examples.states

import com.meituan.meishi.data.lqy.flink.examples.models.SensorReading
import com.meituan.meishi.data.lqy.flink.examples.winFuncs.SensorReadingExample
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object StateExample {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

//    env.setStateBackend(new RocksDBStateBackend())

    env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
//    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500))
//    env.setRestartStrategy(RestartStrategies.failureRateRestart(
//      3,
//      org.apache.flink.api.common.time.Time.seconds(300),
//      org.apache.flink.api.common.time.Time.seconds(5)))

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

    val processStream2 = dataStream.keyBy(_.id)
      //        .process(new TempChangeAlert(10.0))
      //        .flatMap(new TempChangeAlert2(10.0))
      .flatMapWithState[(String, Double, Double), Double] {
        //如果没有状态的话，也就是没有数据来过，那么就将当前温度值数据存入状态
        case (input: SensorReading, None) =>
          (List.empty, Some(input.temperature))
        //如果有状态的话，就应与上次的温度值比较差值，大于阈值就报警
        case (input: SensorReading, lastTemp: Some[Double]) =>
          val diff = (input.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
          } else {
            (List.empty, Some(input.temperature))
          }
      }

    processStream2.print("StateEx")

    env.execute(SensorReadingExample.getClass.getSimpleName)
  }
}

class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  // 保存上次的温度
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
  )

  override def processElement(value: SensorReading,
                              ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context,
                              out: Collector[(String, Double, Double)]): Unit = {
    //获取上次温度
    val lastTemp = lastTempState.value()
    //当前温度与上次温度求差，如果大于阈值，输出报警
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect(value.id, lastTemp, value.temperature)
    }
    lastTempState.update(value.temperature)
  }
}

class TempChangeAlert2(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
//  // 保存上次的温度
//  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
//    new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
//  )

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double](
      "lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading,
                       out: Collector[(String, Double, Double)]): Unit = {
    //获取上次温度
    val lastTemp = lastTempState.value()
    //当前温度与上次温度求差，如果大于阈值，输出报警
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect(value.id, lastTemp, value.temperature)
    }
    lastTempState.update(value.temperature)
  }
}