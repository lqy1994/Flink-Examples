package com.meituan.meishi.data.lqy.scala.examples.tour

object Tuples {

  def main(args: Array[String]): Unit = {
    val ing: Tuple2[String, Int] = ("Sugar", 25)

    val dis = List(("Mercury", 57.9), ("Venus", 108.2), ("Earth", 149.6), ("Mars", 227.9), ("Jupiter", 778.3))
    dis.foreach { tuple => {
      tuple match {
        case ("Mercury", distance) => println(s"Mercury is $distance millions km far from Sun")
        case p if (p._1 == "Venus") => println(s"Venus is ${p._2} millions km far from Sun")
        case p if (p._1 == "Earth") => println(s"Blue planet is ${p._2} millions km far from Sun")
        case _ => println("Too far....")
      }
    }
    }
  }

}
