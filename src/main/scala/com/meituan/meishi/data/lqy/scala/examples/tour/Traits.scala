package com.meituan.meishi.data.lqy.scala.examples.tour

object Traits {

  def main(args: Array[String]): Unit = {
    val intIterator = new IntIterator(10)
    println(intIterator.next())
    println(intIterator.next())
    println(intIterator.hasNext)

  }

  trait Iterator[A] {
    def hasNext: Boolean

    def next(): A
  }

  class IntIterator(to: Int) extends Iterator[Int] {
    private var current = 0
    override def hasNext: Boolean = current < to

    override def next(): Int = {
      if (hasNext) {
        val t = current
        current += 1
        t
      } else
        0
    }
  }

}
