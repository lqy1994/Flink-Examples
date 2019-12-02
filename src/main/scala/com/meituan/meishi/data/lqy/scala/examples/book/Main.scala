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
    List(1, 2, 3, 4) filter isEven foreach println

    println("==============================================================================")

    val dogBreeds = List("Doberman", "Yorkshire Terrier", "Dachshund",
      "Scottish Terrier", "Great Dane", "Portuguese Water Dog")
    for (breed <- dogBreeds if breed contains ("Terrier") if !breed.startsWith("Yorkshire")
         ) println(breed)

    val filteredBreeds = for {
      breed <- dogBreeds
      if breed.contains("Terrier") && !breed.startsWith("Yorkshire")
    } yield breed
    println(filteredBreeds)

    val dogBreeds1 = List(Some("Doberman"), None, Some("Yorkshire Terrier"), Some("Dachshund"), None,
      Some("Scottish Terrier"), None, Some("Great Dane"), Some("Portuguese Water Dog"))
    println("first pass:")
    for {
      breedOption <- dogBreeds1
      breed <- breedOption
      upcasedBreed = breed.toUpperCase()
    } println(upcasedBreed)
    println("second pass:")
    for {
      Some(breed) <- dogBreeds1
      upcasedBreed = breed.toUpperCase()
    } println(upcasedBreed)


  }

  private def isEven(n: Int) = (n % 2 == 0)

}
