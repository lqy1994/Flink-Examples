package com.meituan.meishi.data.lqy.scala.examples.book

import scala.collection.mutable

object Main {

  def main(args: Array[String]): Unit = {
    var s = mutable.Seq("1", "2")
    s +: "3"
    s ++ "4"
    println(s)

    val res = (1 to 100 by 5) filter (s => s % 2 == 0) map (_ * 2) reduce (_ * _)
    println(res)

  }
}
