package com.meituan.meishi.data.lqy.scala.examples.tour

//当某个特质被用于组合类时，被称为混入。
object MixIn {

  def main(args: Array[String]): Unit = {
    val d: D = new D
    println(d.loadMessage)

    val it = new RichStringItr
    it foreach println
  }

  abstract class A {
    val message: String
  }

  class B extends A {
    override val message: String = "I'm an instance of class B"
  }

  trait C extends A {
    def loadMessage: String = message.toUpperCase()
  }

  class D extends B with C

  abstract class AbsIterator {
    type T

    def hasNext: Boolean

    def next(): T
  }

  class StringIterator(s: String) extends AbsIterator {
    type T = String
    private var i = 0

    override def hasNext: Boolean = i < s.length

    override def next() = {
      val ch = s charAt i
      i += 1
      ch.toString
    }
  }

  trait RichIterator extends AbsIterator {
    def foreach(f: T => Unit): Unit = while(hasNext) f(next())
  }

  class RichStringItr extends StringIterator("Scala") with RichIterator
}
