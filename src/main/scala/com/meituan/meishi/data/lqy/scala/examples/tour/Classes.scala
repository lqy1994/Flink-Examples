package com.meituan.meishi.data.lqy.scala.examples.tour

object Classes {

  def main(args: Array[String]): Unit = {
    val point1 = new Point(2, 3)
    point1.move(2, 5)
    println(point1)
    val point2 = new Point(y = 2)

    val pointA = new PointA
    pointA.x_(99)
    pointA.y_(101)


  }

  class Point(var x: Int = 0, var y: Int = 0) {

    def move(dx: Int, dy: Int): Unit = {
      x += dx
      y += dy
    }

    override def toString: String = s"($x,$y)"

  }

  class PointA {
    private var _x = 0
    private var _y = 0
    private val bound = 100

    def x = _x

    def x_(newVal: Int): Unit = {
      if (newVal < bound) _x = newVal else printWarning
    }

    def y = _y

    def y_(newVal: Int): Unit = {
      if (newVal < bound) _y = newVal else printWarning
    }

    private def printWarning = println("WARNING: Out of bounds")

  }

}
