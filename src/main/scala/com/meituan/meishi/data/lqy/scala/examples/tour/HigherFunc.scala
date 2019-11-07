package com.meituan.meishi.data.lqy.scala.examples.tour

object HigherFunc {

  def main(args: Array[String]): Unit = {
    val salaries = Seq(20000, 70000, 40000)
    val doubleSalary = (x: Int) => x * 2
    val newSalaries = salaries.foreach(doubleSalary)
    val newS1 = salaries.map(s => s * 2)
    val news2 = salaries.map(_ * 2)

    val domainName = "www.example.com"

    def getURL = urlBuilder(ssl = true, domainName)

    val endpoint = "users"
    val query = "id=1"
    val url = getURL(endpoint, query) // "https://www.example.com/users?id=1": String
    println(url)

  }

  // 强制转换方法为函数
  case class WeeklyWeatherForecast(temperatures: Seq[Double]) {

    private def convertCtoF(temp: Double) = temp * 1.8 + 32

    def forecastInFahrenheit: Seq[Double] = temperatures.map(convertCtoF) // <-- passing the method convertCtoF
  }

  // 接收函数作为参数的函数
  object SalaryRaiser {
    def smallPromotion(salaries: List[Double]): List[Double] =
      salaries.map(_ * 1.1)

    def greatPromotion(salaries: List[Double]): List[Double] =
      salaries.map(s => s * math.log(s))

    def hugePromotion(salaries: List[Double]): List[Double] =
      salaries.map(Math.pow(_, 2))
  }

  object SalaryRaiser1 {
    private def promotion(salaries: List[Double], function: Double => Double): List[Double] =
      salaries.map(function)

    def smallPromotion(salaries: List[Double]): List[Double] =
      promotion(salaries, s => s * 1.1)

    def greatPromotion(salaries: List[Double]): List[Double] =
      promotion(salaries, s => s * math.log(s))

    def hugePromotion(salaries: List[Double]): List[Double] =
      promotion(salaries, Math.pow(_, 2))
  }

  // 返回函数的函数
  def urlBuilder(ssl: Boolean, domain: String): (String, String) => String = {
    val schema = if (ssl) "https://" else "http://"
    (endpoint: String, query: String) => s"$schema$domain/$endpoint?$query"
  }

}
