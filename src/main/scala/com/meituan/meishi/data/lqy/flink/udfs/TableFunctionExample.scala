package com.meituan.meishi.data.lqy.flink.udfs

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * 一行转多行
 * lateral
 */
object TableFunctionExample extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv = BatchTableEnvironment.create(env)
  env.setParallelism(1)

  val split = new Split(",")
  tableEnv.registerFunction("splitter", split)

  val table = tableEnv.fromDataSet(
    env.fromElements("a,b", "e,f")
    , 'elem)
  tableEnv.registerTable("myTable", table)

  table.joinLateral(split('elem) as ('word, 'len))
    .select('elem, 'word, 'len)
    .toDataSet[Row].print("table")

  tableEnv.sqlQuery(
    """
      |SELECT elem, word, len
      |FROM myTable, LATERAL TABLE (splitter(elem)) AS T(word, len)
      |""".stripMargin).toDataSet[Row].print("sql")

  tableEnv.sqlQuery(
    """
      |SELECT elem, word, len
      |FROM myTable LEFT JOIN
      | LATERAL TABLE (splitter(elem)) AS T(word, len)
      | ON TRUE
      |""".stripMargin)

  env.execute("table func")
}

class Split(sep: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    str.split(sep).foreach(x => collect((x, x.length)))
  }
}
