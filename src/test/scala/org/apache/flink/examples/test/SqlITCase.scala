package org.apache.flink.examples.test

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Test

class SqlITCase extends AbstractTestBase with Serializable {
  val holder = new SeqHolder

  val customerTab = "customer_tab"
  val orderTab = "order_tab"
  val itemTab = "item_tab"
  val pageAccessTab = "pageAccess_tab"
  val pageAccessCountTab = "pageAccessCount_tab"
  val pageAccessSessionTab = "pageAccessSession_tab"

  def registerTables(env: StreamExecutionEnvironment, tEnv: StreamTableEnvironment): Unit = {
    val customer = env
      .fromCollection(holder.customerData)
      .toTable(tEnv)
      .as('c_id, 'c_name, 'c_desc)

    val order = env
      .fromCollection(holder.orderData)
      .toTable(tEnv)
      .as('o_id, 'c_id, 'o_time, 'o_desc)

    tEnv.registerTable(orderTab, order)
    tEnv.registerTable(customerTab, customer)

    val item = env
      .addSource(new EventTimeSourceFunction[(Long, Int, String, String)](holder.itemData))
      .toTable(tEnv, 'onSellTime.rowtime, 'price, 'itemID, 'itemType)
    val pageAccess = env
      .addSource(new EventTimeSourceFunction[(Long, String, String)](holder.pageAccessData))
      .toTable(tEnv, 'accessTime.rowtime, 'region, 'userId)
    val pageAccessCount = env
      .addSource(new EventTimeSourceFunction[(Long, String, Int)](holder.pageAccessCountData))
      .toTable(tEnv, 'accessTime.rowtime, 'region, 'accessCount)
    val pageAccessSession = env
      .addSource(new EventTimeSourceFunction[(Long, String, String)](holder.pageAccessSessionData))
      .toTable(tEnv, 'accessTime.rowtime, 'region, 'userId)

    tEnv.registerTable(itemTab, item)
    tEnv.registerTable(pageAccessTab, pageAccess)
    tEnv.registerTable(pageAccessCountTab, pageAccessCount)
    tEnv.registerTable(pageAccessSessionTab, pageAccessSession)
  }

  class EventTimeSourceFunction[T](dataWithTimestampList: Seq[Either[(Long, T), Long]])
    extends SourceFunction[T] {
    override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
      dataWithTimestampList.foreach {
        case Left(t) => ctx.collectWithTimestamp(t._2, t._1)
        case Right(w) => ctx.emitWatermark(new Watermark(w))
      }
    }

    override def cancel(): Unit = {}
  }

  def initEnv(): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    val settings = EnvironmentSettings.newInstance().useOldPlanner()
      .inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, settings)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    (env, tEnv)
  }

  @Test def testSelect(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv);

    val table = tEnv.sqlQuery(
      """
        |SELECT c_name,
        |CONCAT(c_name, ' come ', c_desc) AS desc
        |FROM customer_tab
        |""".stripMargin
    )

    val stream: DataStream[(Boolean, Row)] = tEnv.toRetractStream[Row](table)
    stream.filter(t => t._1)
      .map(t => t._2)
      .print("")

    env.execute()
  }

  @Test def where(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv);

    val table = tEnv.sqlQuery(
      s"""
         | SELECT c_id, c_name, c_desc
         | FROM $customerTab
         | WHERE c_id = 'c_001' OR c_id = 'c_003'
         |""".stripMargin)

    tEnv.toAppendStream[Row](table).print("")
    env.execute()
  }

  @Test def inSelect(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv);

    val table = tEnv.sqlQuery(
      s"""
         |SELECT c_id, c_name
         |FROM $customerTab
         |WHERE c_id IN (
         |  SELECT c_id
         |  FROM $orderTab
         |)
         |""".stripMargin
    )

    tEnv.toRetractStream[Row](table).map(_._2).print
    env.execute()
  }

  /**
   * 统计每个客户的订单量
   */
  @Test def groupBy(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv);

    val table = tEnv.sqlQuery(
      s"""
         |SELECT c_id, COUNT(o_id) AS o_count
         |FROM $orderTab
         |GROUP BY c_id
         |""".stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  /**
   * 按时间分组，查询每分钟的订单数
   */
  @Test def groupByTime(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv);

    tEnv.registerFunction("user_dateFormat", new UserDateFormat)

    val table = tEnv.sqlQuery(
      s"""
         | SELECT user_dateFormat(o.o_time) AS user_time, COUNT(o_id)
         | FROM $orderTab AS o
         | GROUP BY user_dateFormat(o.o_time)
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  @Test def join(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv);

    val table = tEnv.sqlQuery(
      s"""
         | SELECT c.c_id, c_name, o_id, o_time, c_desc, o_desc
         | FROM $customerTab AS c
         | JOIN $orderTab AS o ON c.c_id = o.c_id
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  @Test def leftJoin(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv);

    val table = tEnv.sqlQuery(
      s"""
         | SELECT *
         | FROM $customerTab AS c
         | LEFT JOIN $orderTab AS o ON o.c_id = c.c_id
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  /**
   * Bounded ROWS OVER Window 每一行元素都视为新的计算行，即，每一行都是一个新的窗口。
   * <p>
   * SELECT
   * agg1(col1) OVER(
   * [PARTITION BY (value_expression1,..., value_expressionN)]
   * ORDER BY timeCol
   * ROWS
   * BETWEEN (UNBOUNDED | rowCount) PRECEDING AND CURRENT ROW) AS colName,
   * ...
   * FROM Tab1
   */
  @Test def boundedRowsOverWindow(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv)
    //我们统计同类商品中当前和当前商品之前2个商品中的最高价格。
    val table = tEnv.sqlQuery(
      s"""
         | SELECT itemID, itemType, onSellTime, price,
         |    MAX(price) OVER (
         |     PARTITION BY itemID
         |     ORDER BY onSellTime
         |     ROWS BETWEEN 2 preceding AND CURRENT ROW) AS maxPrice
         | FROM $itemTab
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  /**
   * Bounded RANGE OVER Window 具有相同时间值的所有元素行视为同一计算行，即，具有相同时间值的所有行都是同一个窗口。
   * <p>
   * SELECT
   * agg1(col1) OVER(
   * [PARTITION BY (value_expression1,..., value_expressionN)]
   * ORDER BY timeCol
   * RANGE
   * BETWEEN (UNBOUNDED | timeInterval) PRECEDING AND CURRENT ROW) AS colName,
   * ...
   * FROM Tab1
   */
  @Test def boundedRangeOverWindow(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv)
    //我们统计同类商品中当前和当前商品之前2分钟商品中的最高价格。
    val table = tEnv.sqlQuery(
      s"""
         | SELECT itemID, itemType, onSellTime, price,
         |    MAX(price) OVER (
         |     PARTITION BY itemType
         |     ORDER BY onSellTime
         |     RANGE BETWEEN INTERVAL '2' MINUTE preceding AND CURRENT ROW) AS maxPrice
         | FROM $itemTab
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  /**
   * Tumble
   * <p>
   * SELECT
   * [gk], - 决定了流是Keyed还是/Non-Keyed;
   * [TUMBLE_START(timeCol, size)], - 窗口开始时间;
   * [TUMBLE_END(timeCol, size)], - 窗口结束时间;
   * agg1(col1),
   * ...
   * aggn(colN)
   * FROM Tab1
   * GROUP BY [gk], TUMBLE(timeCol, size)
   */
  @Test def tumbleGroupWindow(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv)
    //按不同地域统计每2分钟的首页访问量（PV）
    val table = tEnv.sqlQuery(
      s"""
         | SELECT region,
         |    TUMBLE_START(accessTime, INTERVAL '2' MINUTE) AS winStart,
         |    TUMBLE_END  (accessTime, INTERVAL '2' MINUTE) AS winEnd,
         |    COUNT(region) AS pv
         | FROM $pageAccessTab
         | GROUP BY region, TUMBLE(accessTime, INTERVAL '2' MINUTE)
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  /**
   * HOP
   * <p>
   * SELECT
   * [gk],
   * [HOP_START(timeCol, slide, size)] ,
   * [HOP_END(timeCol, slide, size)],
   * agg1(col1),
   * ...
   * aggN(colN)
   * FROM Tab1
   * GROUP BY [gk], HOP(timeCol, slide, size)
   */
  @Test def hopSlideGroupWindow(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv)
    //每5分钟统计近10分钟的页面访问量
    val table = tEnv.sqlQuery(
      s""" SELECT
         |    region,
         |    HOP_START(accessTime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) AS winStart,
         |    HOP_END  (accessTime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) AS winEnd,
         |    SUM(accessCount) AS accessCount
         | FROM $pageAccessCountTab
         | GROUP BY region,
         |  HOP(accessTime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE)
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }

  /**
   * Session
   * <p>
   * SELECT
   * [gk],
   * SESSION_START(timeCol, gap) AS winStart,
   * SESSION_END(timeCol, gap) AS winEnd,  - gap 是窗口数据非活跃周期的时长；
   * agg1(col1),
   * ...
   * aggn(colN)
   * FROM Tab1
   * GROUP BY [gk], SESSION(timeCol, gap)
   */
  @Test def sessionGroupWindow(): Unit = {
    val envs = initEnv()
    val env = envs._1
    val tEnv = envs._2
    registerTables(env, tEnv)
    //按地域统计连续两个访问用户之间的访问时间间隔不超过3分钟的页面访问量
    val table = tEnv.sqlQuery(
      s""" SELECT
         |    region,
         |    SESSION_START(accessTime, INTERVAL '3' MINUTE) AS winStart,
         |    SESSION_END  (accessTime, INTERVAL '3' MINUTE) AS winEnd,
         |    COUNT(region) AS pv
         | FROM $pageAccessSessionTab
         | GROUP BY region,
         | SESSION(accessTime, INTERVAL '3' MINUTE)
         | """.stripMargin
    )
    //过滤掉被覆盖的数据
    tEnv.toRetractStream[Row](table).filter(_._1).map(_._2).print
    env.execute()
  }


}

class UserDateFormat extends ScalarFunction {
  def eval(str: String): String = {
    val time = LocalDateTime.parse(str, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
  }
}
