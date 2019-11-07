package com.meituan.meishi.data.lqy.scala.examples.tour

import scala.util.Random

object Unapply {

  def main(args: Array[String]): Unit = {
    val customer1ID = CustomerID("Sukyoung") // Sukyoung--23098234908
    customer1ID match {
      case CustomerID(name) => println(name) // prints Sukyoung ---> unapply
      case _ => println("Could not extract a CustomerID")
    }
  }

  object CustomerID {
    def apply(name: String) = s"$name--${Random.nextLong}"

    def unapply(customerID: String): Option[String] = {
      val arr: Array[String] = customerID.split("--")
      if (arr.tail.nonEmpty) Some(arr.head) else None
    }

  }

}
