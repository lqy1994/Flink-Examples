package com.meituan.meishi.data.lqy.scala.examples.tour

import scala.math._

object Objects {

  def main(args: Array[String]): Unit = {
    val circle1 = Circle(5.0)
    println(circle1.area)

    val scalaCenterEmail = Email.fromString("scala.center@epfl.ch")
    scalaCenterEmail match {
      case Some(email) => println(
        s"""Registered an email
           |Username: ${email.username}
           |Domain name: ${email.domainName}
  """)
      case None => println("Error: could not parse email")
    }

  }

}

case class Circle(radius: Double) {

  import Circle._

  def area: Double = calculateArea(radius)
}

//伴生对象
object Circle {
  def calculateArea(radius: Double): Double = Pi * pow(radius, 2.0)
}

class Email(val username: String, val domainName: String)

object Email {
  def fromString(emailString: String): Option[Email] = {
    emailString.split('@') match {
      case Array(a, b) => Some(new Email(a, b))
      case _ => None
    }
  }
}