package org.apache.flink.examples.test.data

import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object StreamTestData {

  def getSingletonDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 42L, "Hi"))
    env.fromCollection(data)
  }

  def getSmall3TupleDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Hi"))
    data.+=((2, 2L, "Hello"))
    data.+=((3, 2L, "Hello world"))
    env.fromCollection(data)
  }

  def get3TupleDataStream(env: StreamExecutionEnvironment): DataStream[(Int, Long, String)] = {
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
    env.fromCollection(data)
  }

  def get5TupleDataStream(env: StreamExecutionEnvironment):
  DataStream[(Int, Long, Int, String, Long)] = {

    val data = new mutable.MutableList[(Int, Long, Int, String, Long)]
    data.+=((1, 1L, 0, "Hallo", 1L))
    data.+=((2, 2L, 1, "Hallo Welt", 2L))
    data.+=((2, 3L, 2, "Hallo Welt wie", 1L))
    data.+=((3, 4L, 3, "Hallo Welt wie gehts?", 2L))
    data.+=((3, 5L, 4, "ABC", 2L))
    data.+=((3, 6L, 5, "BCD", 3L))
    data.+=((4, 7L, 6, "CDE", 2L))
    data.+=((4, 8L, 7, "DEF", 1L))
    data.+=((4, 9L, 8, "EFG", 1L))
    data.+=((4, 10L, 9, "FGH", 2L))
    data.+=((5, 11L, 10, "GHI", 1L))
    data.+=((5, 12L, 11, "HIJ", 3L))
    data.+=((5, 13L, 12, "IJK", 3L))
    data.+=((5, 14L, 13, "JKL", 2L))
    data.+=((5, 15L, 14, "KLM", 2L))
    env.fromCollection(data)
  }

  def getSmallNestedTupleDataStream(env: StreamExecutionEnvironment):
  DataStream[((Int, Int), String)] = {
    val data = new mutable.MutableList[((Int, Int), String)]
    data.+=(((1, 1), "one"))
    data.+=(((2, 2), "two"))
    data.+=(((3, 3), "three"))
    env.fromCollection(data)
  }
}