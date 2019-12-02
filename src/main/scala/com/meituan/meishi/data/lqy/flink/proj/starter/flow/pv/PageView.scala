package com.meituan.meishi.data.lqy.flink.proj.starter.flow.pv

import com.meituan.meishi.data.lqy.flink.proj.model.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resources = getClass.getResource("/proj/UserBehavior.csv")

    val dataStream = env.readTextFile(resources.getPath)
        .map(data => {
          val arr = data.split(",")
          UserBehavior(arr(0).trim.toLong, arr(1).trim.toLong,
            arr(2).trim.toInt, arr(3).trim, arr(4).trim.toLong)
        })
        .assignAscendingTimestamps(_.timestamp * 1000)
        .filter(_.behavior == "pv") //只统计pv操作
        .map(_ => ("pv", 1))
        .keyBy(_._1)
        .timeWindow(Time.hours(1))
        .sum(1)

    dataStream.print("pv count")

    env.execute("Page View")
  }
}
