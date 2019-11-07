package com.meituan.meishi.data.lqy.scala.examples.tour

object CaseCls {

  def main(args: Array[String]): Unit = {
    //注意在实例化案例类Book时，并没有使用关键字new，这是因为案例类有一个默认的apply方法来负责对象的创建。
    val frankenstein = Book("978-0486282114")
    val f1 = frankenstein.copy(ISBN = frankenstein.ISBN)
    println(f1 == frankenstein)
  }

  case class Book(ISBN: String)

}
