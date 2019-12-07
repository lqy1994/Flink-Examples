package com.meituan.meishi.data.lqy.flink.udfExample

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * 单行输入
 * 单行输出
 */
object ScalarFunctionExample extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv = BatchTableEnvironment.create(env)

  val hash: HashCode = new HashCode(10)
  tableEnv.registerFunction("hashCode", hash)

  val table = tableEnv.fromDataSet(env.fromElements(("a", "lqy"), ("b", "cde")), 'key, 'vals)
  tableEnv.registerTable("myTable", table)

  table.select('key, hash('vals) as 'hash_code)
      .toDataSet[Row].print("table")

  tableEnv.sqlQuery(
    """
      |SELECT key, hashCode(vals) AS hash_code
      |FROM myTable
      |""".stripMargin).toDataSet[Row].print("sql")

  env.execute("scala r func")
}

class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode() * factor
  }
}