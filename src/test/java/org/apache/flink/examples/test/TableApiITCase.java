package org.apache.flink.examples.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.JavaConverters;
import scala.collection.JavaConverters$;
import scala.util.Either;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class TableApiITCase extends AbstractTestBase {

    private static final String CUSTOMER_TAB = "customer_tab";
    private static final String ORDER_TAB = "order_tab";
    private static final String ITEM_TAB = "item_tab";
    private static final String PAGE_ACCESS_TAB = "pageAccess_tab";
    private static final String PAGE_CNT_TAB = "pageAccessCount_tab";
    private static final String PAGE_SESSION_TAB = "pageAccessSession_tab";

    private Table customerTable;
    private Table orderTable;
    private Table itemTable;
    private Table pageAccessTable;
    private Table pageCountTable;
    private Table pageSessionTable;

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @Test
    public void testTimeZone() {
        TimeZone aDefault = TimeZone.getDefault();
//        ZoneId.of("Asia/Shanghai")
        System.out.println(aDefault);
    }

    @Before
    public void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
        tEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        Map<String, String> configMap = new HashMap<>();
        configMap.put("user.timezone", "GMT+08");

        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(configMap));
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(
                new String[]{
                        "-Duser.timezone=GMT+08"
                }
        ));

        SeqHolder holder = new SeqHolder();

        // 客户表数据
        SingleOutputStreamOperator<Customer> customStream = env
                .addSource(new EventModelSource<>(JavaConverters.asJavaCollection(holder.customerData())))
                .returns(TypeInformation.of(new TypeHint<Tuple3<String, String, String>>() {
                }))
                .map(new MapFunction<Tuple3<String, String, String>, Customer>() {
                    @Override
                    public Customer map(Tuple3<String, String, String> t) throws Exception {
                        return new Customer(t._1(), t._2(), t._3());
                    }
                });
        tEnv.registerDataStream(CUSTOMER_TAB, customStream, "cId, cName, cDesc");
        customerTable = tEnv.scan(CUSTOMER_TAB);

        SingleOutputStreamOperator<Order> orderStream = env
                .addSource(new EventModelSource<Tuple4<String, String, String, String>>(JavaConverters.asJavaCollection(holder.orderData())))
                .returns(TypeInformation.of(new TypeHint<Tuple4<String, String, String, String>>() {
                }))
                .map(new MapFunction<Tuple4<String, String, String, String>, Order>() {
                    @Override
                    public Order map(Tuple4<String, String, String, String> t) throws Exception {
                        return new Order(t._1(), t._2(), t._3(), t._4());
                    }
                });
        tEnv.registerDataStream(ORDER_TAB, orderStream, "oId, cId, oTime, oDesc");
        orderTable = tEnv.scan(ORDER_TAB);

        //商品销售表数据
        DataStream<ItemData> itemStream = env
                .addSource(new EventDataSource<>(JavaConverters.asJavaCollection(holder.itemData())))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, String, String>>() {
                }))
                .map(new MapFunction<Tuple4<Object, Object, String, String>, ItemData>() {
                    @Override
                    public ItemData map(Tuple4<Object, Object, String, String> t) throws Exception {
                        return new ItemData((Long) t._1(), (Integer) t._2(), (String) t._3(), (String) t._4());
                    }
                });
        tEnv.registerDataStream(ITEM_TAB, itemStream, "onSellTime.rowtime, price, itemID, itemType");
        itemTable = tEnv.scan(ITEM_TAB);

        //页面访问表
        Collection<Either<Tuple2<Object, Tuple3<Object, String, String>>, Object>> pageAccessData =
                JavaConverters.asJavaCollection(holder.pageAccessData());
        SingleOutputStreamOperator<PageAccess> pageAccessStream = env
                .addSource(new EventDataSource<>(pageAccessData))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Object, String, String>>() {
                }))
                .map(new MapFunction<Tuple3<Object, String, String>, PageAccess>() {
                    @Override
                    public PageAccess map(Tuple3<Object, String, String> t) throws Exception {
                        return new PageAccess((Long) t._1(), t._2(), t._3());
                    }
                });
        tEnv.registerDataStream(PAGE_ACCESS_TAB, pageAccessStream, "accessTime.rowtime, region, userId");
        pageAccessTable = tEnv.scan(PAGE_ACCESS_TAB);

        //页面访问量表数据2
        Collection<Either<Tuple2<Object, Tuple3<Object, String, Object>>, Object>> pageAccessCount =
                JavaConverters.asJavaCollection(holder.pageAccessCountData());
        SingleOutputStreamOperator<PageAccessCount> pageAccessCountStream = env
                .addSource(new EventDataSource<>(pageAccessCount))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Object, String, Object>>() {
                }))
                .map(new MapFunction<Tuple3<Object, String, Object>, PageAccessCount>() {
                    @Override
                    public PageAccessCount map(Tuple3<Object, String, Object> t) throws Exception {
                        return new PageAccessCount((Long) t._1(), t._2(), (Integer) t._3());
                    }
                });
        tEnv.registerDataStream(PAGE_CNT_TAB, pageAccessCountStream, "accessTime.rowtime, region, accessCount");
        pageCountTable = tEnv.scan(PAGE_CNT_TAB);

        //页面访问表数据3
        Collection<Either<Tuple2<Object, Tuple3<Object, String, String>>, Object>> pageAccessSession =
                JavaConverters.asJavaCollection(holder.pageAccessSessionData());
        SingleOutputStreamOperator<PageAccessSession> pageAccessSessionStream = env
                .addSource(new EventDataSource<>(pageAccessSession))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Object, String, String>>() {
                }))
                .map(new MapFunction<Tuple3<Object, String, String>, PageAccessSession>() {
                    @Override
                    public PageAccessSession map(Tuple3<Object, String, String> t) throws Exception {
                        return new PageAccessSession((Long) t._1(), t._2(), t._3());
                    }
                });
        tEnv.registerDataStream(PAGE_SESSION_TAB, pageAccessSessionStream, "accessTime.rowtime, region, userId");
        pageSessionTable = tEnv.scan(PAGE_SESSION_TAB);
    }

    @After
    public void after() throws Exception {
        env.execute("Table OverView ITCase ");
    }

    @Test
    public void testExec() {
        Assert.assertNotNull(tEnv);
    }


    /**
     * GROUP BY
     */
    @Test
    public void testGroupBy() {
        Table table = orderTable
                .groupBy("cId")
                .select("cId, COUNT(oId)");

        tEnv.toRetractStream(table, Row.class).print();
        Assert.assertNotNull(table);
    }

    /**
     * Intersect只在Batch模式下进行支持，Stream模式下我们可以利用双流JOIN来实现，如：在customer_tab查询已经下过订单的客户信息，如下：
     */
    @Test
    public void testIntersect() {
        Table distinctCId = orderTable
                .groupBy("cId")
                .select("cId AS o_c_id");

        Table table = customerTable
                .join(distinctCId, "cId === o_c_id")
                .select("cId, cName, cDesc");

        tEnv.toRetractStream(table, Row.class).print();
        Assert.assertNotNull(table);
    }

    /**
     * Minus只在Batch模式下进行支持，Stream模式下我们可以利用双流JOIN来实现，如：在customer_tab查询没有下过订单的客户信息，如下：
     */
    @Test
    public void testMinus() {
        Table distinctCId = orderTable
                .groupBy("cId")
                .select("cId AS o_c_id");

        Table table = customerTable
                .leftOuterJoin(distinctCId, "cId === o_c_id")
                .where("o_c_id.isNull")
                .select("cId, cName, cDesc");

        tEnv.toRetractStream(table, Row.class).print();
    }

    //============================= JOIN =======================================================

    /**
     * JOIN
     */
    @Test
    public void testJoin() {
        Table join = customerTable
                .join(orderTable.select("oId, cId AS ocId, oTime, oDesc"), "cId === ocId");
        tEnv.toRetractStream(join, Row.class).print();
    }

    /**
     * Time-Interval JOIN
     * <p>
     * SELECT ... FROM t1 JOIN t2  ON t1.key = t2.key AND TIMEBOUND_EXPRESSION
     * <p>
     * TIMEBOUND_EXPRESSION 有两种写法，如下：
     * L.time between LowerBound(R.time) and UpperBound(R.time)
     * R.time between LowerBound(L.time) and UpperBound(L.time)
     * 带有时间属性(L.time/R.time)的比较表达式。
     */
    @Test
    public void testTimeIntervalJoin() {
        SingleOutputStreamOperator orderStream = env.fromElements(
                org.apache.flink.api.java.tuple.Tuple3.of("001", "iphone", new Timestamp(1545800002000L)),
                org.apache.flink.api.java.tuple.Tuple3.of("002", "mac", new Timestamp(1545800003000L)),
                org.apache.flink.api.java.tuple.Tuple3.of("003", "book", new Timestamp(1545800004000L)),
                org.apache.flink.api.java.tuple.Tuple3.of("004", "cup", new Timestamp(1545800018000L))
        ).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<org.apache.flink.api.java.tuple.Tuple3<String, String,
                        Timestamp>>(
                        Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(org.apache.flink.api.java.tuple.Tuple3<String, String, Timestamp> element) {
                        return element.f2.getTime();
                    }
                });
        tEnv.registerDataStream("Orders", orderStream, "orderId, productName, orderTime.rowtime");

        SingleOutputStreamOperator paymentStream = env.fromElements(
                org.apache.flink.api.java.tuple.Tuple3.of("001", "alipay", new Timestamp(1545803501000L)),
                org.apache.flink.api.java.tuple.Tuple3.of("002", "card", new Timestamp(1545803602000L)),
                org.apache.flink.api.java.tuple.Tuple3.of("003", "card", new Timestamp(1545803610000L)),
                org.apache.flink.api.java.tuple.Tuple3.of("004", "alipay", new Timestamp(1545803611000L))
        ).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<org.apache.flink.api.java.tuple.Tuple3<String, String,
                        Timestamp>>(
                        Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(org.apache.flink.api.java.tuple.Tuple3<String, String, Timestamp> element) {
                        return element.f2.getTime();
                    }
                });
        tEnv.registerDataStream("Payment", paymentStream, "orderId, payType, payTime.rowtime");

        Table orders = tEnv.scan("Orders");
        Table payment = tEnv.scan("Payment");

        Table table = tEnv.sqlQuery(
                "SELECT " +
                        "   o.orderId, \n" +
                        "   o.productName, \n" +
                        "   p.payType, \n" +
                        "   o.orderTime, \n" +
                        "   CAST(payTime AS TIMESTAMP) AS payTime \n" +
                        "FROM Orders AS o JOIN Payment AS p ON \n" +
                        "   o.orderId = p.orderId AND \n" +
                        "   p.payTime BETWEEN orderTime AND orderTime + INTERVAL '1' HOUR \n"
        );

        //tEnv.toRetractStream(table, Row.class).print("");

        Table orders1 = orders
                .select("orderId AS o_orderId, productName, orderTime");

        Table join = orders1
                .join(payment)
                .where("o_orderId = orderId && payTime >= orderTime && payTime <= orderTime + 1.hours")
                .select("o_orderId, productName, payType, orderTime, payTime.cast(SQL_TIMESTAMP) AS payTime");

        tEnv.toRetractStream(join, Row.class).print("");
    }

    /**
     * Lateral 是左边Table与一个UDTF进行JOIN
     * val udtf = new UDTF
     * val result = source.join(udtf('c) as ('d, 'e))
     */
    @Test
    public void testLateralJoin() {
        DataStream<String> userStream = env.fromElements("Sunny#8", "Kevin#36", "Panpan#36");
        tEnv.registerDataStream("userTab", userStream, "data");

        tEnv.registerFunction("SplitTVF", new SplitTVF());

        Table query = tEnv.sqlQuery(
                "SELECT data, name, age \n" +
                        "FROM userTab, LATERAL TABLE(SplitTVF(data)) AS T(name, age)"
        );

        //tEnv.toAppendStream(query, Row.class).print();

        Table userTab = tEnv.scan("userTab");
        Table join = userTab
                .joinLateral("SplitTVF(data) AS (name, age)")
                .select("data, name, age");
        tEnv.toAppendStream(join, Row.class).print();
    }

    /**
     * Temporal Table JOIN
     */
    @Test
    public void testTemporalJoin() {
        SingleOutputStreamOperator<Row> orderStream = env.fromElements(
                Row.of(2L, "Euro", new Timestamp(2L)),
                Row.of(1L, "US Dollar", new Timestamp(3L)),
                Row.of(50L, "Yen", new Timestamp(4L)),
                Row.of(3L, "Euro", new Timestamp(5L))
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Row element) {
                return ((Timestamp) element.getField(2)).getTime();
            }
        });
        tEnv.registerDataStream("Orders", orderStream, "amount, currency, rowtime.rowtime");
        Table orders = tEnv.scan("Orders");

        SingleOutputStreamOperator<Row> ratesHistoryStream = env.fromElements(
                Row.of("US Dollar", 102L, new Timestamp(1L)),
                Row.of("Euro", 114L, new Timestamp(1L)),
                Row.of("Yen", 1L, new Timestamp(1L)),
                Row.of("Euro", 116L, new Timestamp(5L)),
                Row.of("Euro", 119L, new Timestamp(7L))
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Row element) {
                return ((Timestamp) element.getField(2)).getTime();
            }
        });
        tEnv.registerDataStream("RatesHistory", ratesHistoryStream, "currency, rate, rowtime.rowtime");
        Table ratesHistory = tEnv.scan("RatesHistory");

        TemporalTableFunction ratesFunction = ratesHistory.createTemporalTableFunction("rowtime", "currency");
        tEnv.registerFunction("Rates", ratesFunction);

        Table query = tEnv.sqlQuery(
                "SELECT \n" +
                        "   o.currency, o.amount, r.rate, " +
                        "   o.amount * r.rate AS yen_amount " +
                        "FROM Orders o, LATERAL TABLE (Rates(o.rowtime)) AS r " +
                        "WHERE r.currency = o.currency "
        );

//        tEnv.toRetractStream(query, Row.class).print();

        Table table = orders.select("amount, currency AS o_currency, rowtime AS o_rowtime")
                .joinLateral("Rates(o_rowtime) AS (r_currency, r_rate, r_rowtime)", "r_currency = o_currency")
                .select("o_currency, amount, r_rate, amount * r_rate AS yen_amount");
        tEnv.toRetractStream(table, Row.class).print();
    }

    /**
     * Over Window
     */
    @Test
    public void testOverWindow() {
        //ROWS OVER
        Table table = itemTable
                .window(Over.partitionBy("itemType").orderBy("onSellTime").preceding("2.rows").following(" " +
                        "CURRENT_ROW ").as("w"))
                .select("itemID, itemType, onSellTime, price, price.max OVER w AS maxPrice");
        tEnv.toRetractStream(table, Row.class).print("1");

        //RANGE OVER
        Table table1 = itemTable
                .window(Over.partitionBy("itemType").orderBy("onSellTime").preceding("2.minutes").following(" " +
                        "CURRENT_RANGE ").as("w"))
                .select("itemID, itemType, onSellTime, price, price.max OVER w AS maxPrice");
        tEnv.toRetractStream(table1, Row.class).print("2");

    }

    /**
     * Group Window
     */
    @Test
    public void testGroupWindow() {
        //Tumble
        Table tumble = pageAccessTable
                .window(Tumble.over("2.minute").on("accessTime").as("w"))
                .groupBy("w, region")
                .select("region, w.start, w.end, region.count AS pv");
        tEnv.toRetractStream(tumble, Row.class).print("T");

        //Hop(Slide)
        Table slide = pageCountTable
                .window(Slide.over("10.minutes").every("5.minute").on("accessTime").as("w"))
                .groupBy("w")
                .select("w.start, w.end, accessCount.sum AS accessCount");
        tEnv.toRetractStream(slide, Row.class).print("H");

        //Session
        Table session = pageSessionTable
                .window(Session.withGap("3.minute").on("accessTime").as("w"))
                .groupBy("w, region")
                .select("region, w.start, w.end, region.count AS pv");
        tEnv.toRetractStream(session, Row.class).print("S");

    }

    //============================= WINDOW =======================================================

    /**
     * 嵌套Window
     */
    @Test
    public void testEmbedWindow() {
        Table table = pageAccessTable
                .window(Tumble.over("2.minute").on("accessTime").as("w1"))
                .groupBy("w1")
                .select("w1.rowtime AS rowtime, region.count AS pv")
                .window(Session.withGap("3.minute").on("rowtime").as("w2"))
                .groupBy("w2")
                .select("pv.sum");

        tEnv.toRetractStream(table, Row.class).print("");
    }

    public static class SplitTVF extends TableFunction<SimpleUser> {

        public void eval(String user) {
            if (user.contains("#")) {
                String[] split = user.split("#");
                collect(new SimpleUser(split[0], Integer.valueOf(split[1])));
            }
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SimpleUser {
        private String name;
        private Integer age;
    }

    public static class EventModelSource<T> extends RichParallelSourceFunction<T> {

        Collection<T> data;

        public EventModelSource(Collection<T> itemCollection) {
            this.data = itemCollection;
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            for (T datum : data) {
                ctx.collect(datum);
            }
        }

        @Override
        public void cancel() {

        }

    }

    public static class EventDataSource<T> extends RichParallelSourceFunction<T> {

        Collection<Either<Tuple2<Object, T>, Object>> data;

        public EventDataSource(Collection<Either<Tuple2<Object, T>, Object>> itemCollection) {
            this.data = itemCollection;
        }

        @Override
        public void run(SourceContext<T> ctx) throws Exception {
            java.util.Iterator<Either<Tuple2<Object, T>, Object>> iterator = data.iterator();
            while (iterator.hasNext()) {
                Either<Tuple2<Object, T>, Object> either = iterator.next();
                if (either.isLeft()) {
                    ctx.collectWithTimestamp(either.left().get()._2(), (long) either.left().get()._1());
                } else {
                    ctx.emitWatermark(new Watermark((Long) either.right().get()));
                }
            }
        }

        @Override
        public void cancel() {
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Customer {
        private String cId;
        private String cName;
        private String cDesc;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        private String oId;
        private String cId;
        private String oTime;
        private String oDesc;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ItemData {
        private Long onSellTime;
        private Integer price;
        private String itemID;
        private String itemType;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PageAccess {
        private Long accessTime;
        private String region;
        private String userId;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PageAccessCount {
        private Long accessTime;
        private String region;
        private Integer accessCount;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PageAccessSession {
        private Long accessTime;
        private String region;
        private String userId;
    }

}
