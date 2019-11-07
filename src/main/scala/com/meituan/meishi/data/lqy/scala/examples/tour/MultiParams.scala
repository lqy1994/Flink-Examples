package com.meituan.meishi.data.lqy.scala.examples.tour

import scala.concurrent.ExecutionContext

object MultiParams {

  // def foldLeft[B](z: B)(op: (B, A) => B): B
  def main(args: Array[String]): Unit = {
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(numbers.foldLeft(0)((m, n) => m * n))

    val numFunc = numbers.foldLeft(List[Int]()) _
    val squares = numFunc((xs, x) => xs :+ x * x)
    println(squares.toString())
  }

  // 隐式（IMPLICIT）参数
  def execute(args: Int)(implicit ec: ExecutionContext) = ???
}
