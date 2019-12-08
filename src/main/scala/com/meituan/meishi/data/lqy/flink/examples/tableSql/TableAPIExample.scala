package com.meituan.meishi.data.lqy.flink.examples.tableSql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment

object TableAPIExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    env.execute("Table API")
  }
}
