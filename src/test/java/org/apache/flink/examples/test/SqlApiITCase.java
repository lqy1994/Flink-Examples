package org.apache.flink.examples.test;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import com.meituan.meishi.data.lqy.flink.examples.utils.RichRowTypeInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.collection.JavaConverters;
import scala.util.Either;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Sql overview test.
 */
public class SqlApiITCase extends AbstractTestBase {

    StreamExecutionEnvironment env;
    StreamTableEnvironment tEnv;

    @Before
    public void before() throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        Map<String, String> configMap = new HashMap<>();
        configMap.put("user.timezone", "GMT+08");
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(configMap));

        // 客户表数据
        DataStreamSource<Row> customStream = env.fromElements(
                Row.of("c_001", "Kevin", "from JiLin"),
                Row.of("c_002", "Sunny", "from JiLin"),
                Row.of("c_003", "JinCheng", "from HeBei")
        );
        tEnv.registerDataStream("customer_tab", customStream, "c_id, c_name, c_desc");

        //订单表数据
        DataStreamSource<Row> orderStream = env.fromElements(
                Row.of("o_001", "c_002", "2018-11-05 10:01:01", "iphone"),
                Row.of("o_002", "c_001", "2018-11-05 10:01:55", "ipad"),
                Row.of("o_003", "c_001", "2018-11-05 10:03:44", "flink book")
        );
        tEnv.registerDataStream("order_tab", orderStream, "o_id, c_id, o_time, o_desc");

        SeqHolder holder = new SeqHolder();
        //商品销售表数据
        Collection<Either<Tuple2<Object, Tuple4<Object, Object, String, String>>, Object>> itemData
                = JavaConverters.asJavaCollection(holder.itemData());

        DataStream<ItemData> itemStream = env
                .addSource(new EventDataSource<Tuple4<Object, Object, String, String>>(itemData))
                .returns(TypeInformation.of(new TypeHint<Tuple4<Object, Object, String, String>>() {
                }))
                .map(new MapFunction<Tuple4<Object, Object, String, String>, ItemData>() {
                    @Override
                    public ItemData map(Tuple4<Object, Object, String, String> t) throws Exception {
                        return new ItemData((Long) t._1(), (Integer) t._2(), t._3(), t._4());
                    }
                });
        tEnv.registerDataStream("item_tab", itemStream, "onSellTime.rowtime, price, itemID, itemType");

        //页面访问表
        Collection<Either<Tuple2<Object, Tuple3<Object, String, String>>, Object>> pageAccessData =
                JavaConverters.asJavaCollection(holder.pageAccessData());
        DataStream<PageAccess> pageAccessStream = env
                .addSource(new EventDataSource<Tuple3<Object, String, String>>(pageAccessData))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Object, String, String>>() {
                }))
                .map(new MapFunction<Tuple3<Object, String, String>, PageAccess>() {
                    @Override
                    public PageAccess map(Tuple3<Object, String, String> t) throws Exception {
                        return new PageAccess((Long) t._1(), t._2(), t._3());
                    }
                });
        tEnv.registerDataStream("pageAccess_tab", pageAccessStream, "accessTime.rowtime, region, userId");

        //页面访问量表数据2
        Collection<Either<Tuple2<Object, Tuple3<Object, String, Object>>, Object>> pageAccessCount =
                JavaConverters.asJavaCollection(holder.pageAccessCountData());
        DataStream<PageAccessCount> pageAccessCountStream = env
                .addSource(new EventDataSource<Tuple3<Object, String, Object>>(pageAccessCount))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Object, String, Object>>() {
                }))
                .map(new MapFunction<Tuple3<Object, String, Object>, PageAccessCount>() {
                    @Override
                    public PageAccessCount map(Tuple3<Object, String, Object> t) throws Exception {
                        return new PageAccessCount((Long) t._1(), t._2(), (Integer) t._3());
                    }
                });
        tEnv.registerDataStream("pageAccessCount_tab", pageAccessCountStream, "accessTime.rowtime, region, " +
                "accessCount");

        //页面访问表数据3
        Collection<Either<Tuple2<Object, Tuple3<Object, String, String>>, Object>> pageAccessSession =
                JavaConverters.asJavaCollection(holder.pageAccessSessionData());
        DataStream<PageAccessSession> pageAccessSessionStream = env
                .addSource(new EventDataSource<Tuple3<Object, String, String>>(pageAccessSession))
                .returns(TypeInformation.of(new TypeHint<Tuple3<Object, String, String>>() {
                }))
                .map(new MapFunction<Tuple3<Object, String, String>, PageAccessSession>() {
                    @Override
                    public PageAccessSession map(Tuple3<Object, String, String> t) throws Exception {
                        return new PageAccessSession((Long) t._1(), t._2(), t._3());
                    }
                });
        tEnv.registerDataStream("pageAccessSession_tab", pageAccessSessionStream, "accessTime.rowtime, region, userId");

    }

    @After
    public void after() throws Exception {
        env.execute("SQL OverView ITCase ");
    }

    /**
     * SELECT
     *
     * @throws Exception
     */
    @Test
    public void testSelect() throws Exception {
        Table table = tEnv.sqlQuery(
                "SELECT c_name, " +
                "CONCAT(c_name, ' come ', c_desc) as desc " +
                "FROM customer_tab"
        );

        tEnv
                .toRetractStream(table, Row.class)
                .filter(t -> t.f0)
                .map(new MapFunction<org.apache.flink.api.java.tuple.Tuple2<Boolean, Row>, Row>() {
                    @Override
                    public Row map(org.apache.flink.api.java.tuple.Tuple2<Boolean, Row> t) throws Exception {
                        return t.f1;
                    }
                }).print("");
    }

    /**
     * WHERE
     *
     * @throws Exception
     */
    @Test
    public void testWhere() throws Exception {
        Table table = tEnv.sqlQuery(
                "SELECT c_id, " +
                 "	c_name, " +
                 "	c_desc " +
                 "FROM customer_tab " +
                 "WHERE c_id = 'c_001' " +
                 "	OR c_id = 'c_003' "
        );
        //SELECT c_id, c_name, c_desc FROM customer_tab WHERE c_id IN ('c_001', 'c_003')
        tEnv.toAppendStream(table, Row.class).print("");
    }

    /**
     * GROUP BY
     *
     * @throws Exception
     */
    @Test
    public void testGroupBy() throws Exception {
        Table table = tEnv.sqlQuery(
                "SELECT c_id, " +
                "	count(o_id) AS o_count " +
                "FROM order_tab " +
                "GROUP BY c_id "
        );
        tEnv.toRetractStream(table, Row.class).filter(t -> t.f0).print("");

        System.out.println("--------------------------------------------");

        Table table1 = tEnv.sqlQuery(
                "SELECT SUBSTRING(o_time, 1, 16) AS o_time_min, " +
                "	count(o_id) AS o_count " +
                "FROM order_tab " +
                "GROUP BY SUBSTRING(o_time, 1, 16)"
        );

        tEnv.toRetractStream(table1, Row.class).print("");

    }

    /**
     * UNION ALL(不去重) & UNION(去重)
     */
    @Test
    public void testUnionAll() {
        Table table = tEnv.sqlQuery(
                "SELECT c_id, c_name, c_desc  FROM customer_tab \n" +
                "UNION ALL \n" +
                "SELECT c_id, c_name, c_desc  FROM customer_tab "
        );

        tEnv.toRetractStream(table, Row.class).print("");

        Table table1 = tEnv.sqlQuery(
                "SELECT c_id, c_name, c_desc  FROM customer_tab \n" +
                "UNION \n" +
                "SELECT c_id, c_name, c_desc  FROM customer_tab "
        );

        tEnv.toRetractStream(table1, Row.class).print("UNION");
    }

    /**
     * JOIN - INNER JOIN
     * LEFT JOIN - LEFT OUTER JOIN
     * RIGHT JOIN - RIGHT OUTER JOIN
     * FULL JOIN - FULL OUTER JOIN
     */
    @Test
    public void testJoin() {
        //1. INNER JOIN只选择满足ON条件的记录，我们查询customer_tab 和 order_tab表，将有订单的客户和订单信息选择出来，如下：
        //SELECT * FROM customer_tab AS c JOIN order_tab AS o ON o.c_id = c.c_id
        Table joinResult = tEnv.sqlQuery(
                "SELECT *" +
                "FROM customer_tab AS c JOIN order_tab AS o ON o.c_id = c.c_id "
        );

//		tEnv.toAppendStream(joinResult, Row.class).print("");

        System.out.println("---------------------");

        Table leftJoin = tEnv.sqlQuery(
                "SELECT * FROM customer_tab AS c LEFT JOIN order_tab AS o ON o.c_id = c.c_id"
        );
//		tEnv.toRetractStream(leftJoin, Row.class).filter(t -> t.f0).print("L");

//		RIGHT JOIN 相当于 LEFT JOIN 左右两个表交互一下位置。FULL JOIN相当于 RIGHT JOIN 和 LEFT JOIN 之后进行UNION ALL操作。
    }

    //============================= OVER WINDOW =======================================================

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
    @Test
    public void testBoundedRowsOverWindow() {
        //我们统计同类商品中当前和当前商品之前2个商品中的最高价格。
        Table table = tEnv.sqlQuery(
                "SELECT  \n" +
                "    itemID,\n" +
                "    itemType, \n" +
                "    onSellTime, \n" +
                "    price,  \n" +
                "    MAX(price) OVER (\n" +
                "        PARTITION BY itemType \n" +
                "        ORDER BY onSellTime \n" +
                "        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS maxPrice\n" +
                "FROM item_tab"
        );

        tEnv.toRetractStream(table, MaxPriceItem.class).print("");
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
    @Test
    public void testBoundedRangeOverWindow() {
        //我们统计同类商品中当前和当前商品之前2分钟商品中的最高价格。
        Table table = tEnv.sqlQuery(
                "SELECT \n" +
                "    itemID,\n" +
                "    itemType, \n" +
                "    onSellTime, \n" +
                "    price,  \n" +
                "    MAX(price) OVER (\n" +
                "    	PARTITION BY itemType \n" +
                "    	ORDER BY onSellTime \n" +
                "    	RANGE \n" +
                "    	BETWEEN INTERVAL '2' MINUTE PRECEDING AND CURRENT ROW) AS maxPrice\n" +
                "FROM item_tab"
        );

        tEnv.toRetractStream(table, MaxPriceItem.class).print("");
    }

    //============================= GROUP WINDOW =======================================================

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
    @Test
    public void testTumbleGroupWindow() throws Exception {
        //利用pageAccess_tab测试数据，按不同地域统计每5分钟的淘宝首页的访问量(PV)。
        Table table = tEnv.sqlQuery(
                "SELECT \n" +
                "    region,\n" +
                "    TUMBLE_START(accessTime, INTERVAL '5' MINUTE) AS winStart, \n" +
                "    TUMBLE_END  (accessTime, INTERVAL '5' MINUTE) AS winEnd, \n" +
                "    COUNT(region) AS pv  \n" +
                "FROM pageAccess_tab \n" +
                "GROUP BY region, TUMBLE(accessTime, INTERVAL '5' MINUTE) \n"
        );

        SingleOutputStreamOperator<RegionPv> regionPv = tEnv
                .toRetractStream(table, RegionPv.class)
                .filter(t -> t.f0)
                .map(t -> t.f1);

//        SingleOutputStreamOperator<NewRegionPv> newRegionPv = regionPv.map(pv -> {
//            NewRegionPv newPv = new NewRegionPv();
//            newPv.setRegion(pv.getRegion());
//            newPv.setWinEnd(pv.getWinStart().toLocalDateTime());
//            newPv.setWinEnd(pv.getWinEnd().toLocalDateTime());
//            newPv.setPv(pv.getPv());
//            return newPv;
//        });

        SingleOutputStreamOperator<NewRegionPv> newRegionPv = regionPv.map(pv -> {
            NewRegionPv newPv = new NewRegionPv();
            newPv.setRegion(pv.getRegion());
            newPv.setWinEnd(pv.getWinStart().toLocalDateTime()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            newPv.setWinEnd(pv.getWinEnd().toLocalDateTime()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            newPv.setPv(pv.getPv());
            return newPv;
        });

        tEnv.registerDataStream("newRegionPv", newRegionPv, "region, winStart, winEnd, pv");

//        tEnv.toRetractStream(table, RegionPv.class).print("");

        testKafkaConnector();

//        testElasticsearchConnector();

//        testJDBCAppendConnector();
    }

    @Test
    public void testJsonSchema() throws JsonMappingException {
        String schema = RichRowTypeInfo
                .bulider()
                .field("region", Types.STRING(), "1")
                .field("winStart", Types.SQL_TIMESTAMP(), "2")
                .field("winEnd", Types.SQL_TIMESTAMP(), "3")
                .field("pv", Types.LONG(), "4")
                .build()
                .toJsonSchema();
        System.out.println(schema);
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
    @Test
    public void testHopGroupWindow() {
        //利用pageAccessCount_tab测试数据，我们需要每5分钟统计近10分钟的页面访问量(PV).
        Table table = tEnv.sqlQuery(
                "SELECT \n" +
                "    HOP_START(accessTime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) AS winStart, \n" +
                "    HOP_END  (accessTime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) AS winEnd, \n" +
                "    SUM(accessCount) AS accessCount \n" +
                "FROM pageAccessCount_tab \n" +
                "GROUP BY HOP(accessTime, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) \n"
        );

        tEnv.toRetractStream(table, AccessCountSum.class).print("");
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
    @Test
    public void testSessionGroupWindow() {
        //利用pageAccessSession_tab测试数据，我们按地域统计连续的两个访问用户之间的访问时间间隔不超过3分钟的的页面访问量(PV).
        Table table = tEnv.sqlQuery(
                "SELECT  \n" +
                "    region, \n" +
                "    SESSION_START(accessTime, INTERVAL '3' MINUTE) AS winStart,  \n" +
                "    SESSION_END  (accessTime, INTERVAL '3' MINUTE) AS winEnd, \n" +
                "    COUNT(region) AS pv  \n" +
                "FROM pageAccessSession_tab\n" +
                "GROUP BY region, SESSION(accessTime, INTERVAL '3' MINUTE)"
        );

        tEnv.toRetractStream(table, RegionPv.class).print("");
    }


    /**
     * KafkaConnector
     */
    public void testKafkaConnector() throws Exception {

        Table regionPv = tEnv.scan("newRegionPv");

        Kafka kafka = new Kafka()
                .version("universal")
                .topic("flink-sql-api-region-pv")
                .startFromEarliest()
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
                .property("group.id", "consume-kafka");

        ObjectMapper mapper = new ObjectMapper();
        JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(mapper);
        JsonSchema schema = schemaGen.generateSchema(RegionPv.class);

//        TableSchema tableSchema = TableSchema.builder()
//                .field("region", DataTypes.STRING())
//                .field("winStart", DataTypes.TIMESTAMP())
//                .field("winEnd", DataTypes.TIMESTAMP())
//                .field("pv", DataTypes.BIGINT())
//                .build();

//        Json json = new Json()
//                .failOnMissingField(true)
//                .schema(
//                        TableSchema.builder()
//                                .field("region", DataTypes.STRING())
//                                .field("winStart", DataTypes.TIMESTAMP())
//                                .field("winEnd", DataTypes.TIMESTAMP())
//                                .field("pv", DataTypes.BIGINT())
//                                .build()
//                                .toRowType()
//                )
//                .jsonSchema(
//                        RichRowTypeInfo.bulider()
//                                .field("region", Types.STRING(), "1")
//                                .field("winStart", Types.SQL_TIMESTAMP(), "2")
//                                .field("winEnd", Types.SQL_TIMESTAMP(), "3")
//                                .field("pv", Types.LONG(), "4")
//                                .build()
//                                .toJsonSchema()
//                );
//
//
//        tEnv
//                .connect(kafka)
//                .withFormat(json)
//                .withSchema(
//                        new Schema()
//                                .field("region", Types.STRING())
//                                .field("winStart", Types.SQL_TIMESTAMP())
////                                .rowtime(new Rowtime()
////                                        .timestampsFromField("winStart")
////                                        .watermarksPeriodicBounded(60000)
////                                )
//                                .field("winEnd", Types.SQL_TIMESTAMP())
//                                .field("pv", Types.LONG())
//                )
//                .inAppendMode()
//                .registerTableSink("KafkaRegionPvSinkTable");

        Json json = new Json()
                .failOnMissingField(true)
                .schema(
                        TableSchema.builder()
                                .field("region", DataTypes.STRING())
                                .field("winStart", DataTypes.STRING())
                                .field("winEnd", DataTypes.STRING())
                                .field("pv", DataTypes.BIGINT())
                                .build()
                                .toRowType()
                )
                .jsonSchema(
                        RichRowTypeInfo.bulider()
                                .field("region", Types.STRING(), "1")
                                .field("winStart", Types.STRING(), "2")
                                .field("winEnd", Types.STRING(), "3")
                                .field("pv", Types.LONG(), "4")
                                .build()
                                .toJsonSchema()
                );


        tEnv
                .connect(kafka)
                .withFormat(json)
                .withSchema(
                        new Schema()
                                .field("region", Types.STRING())
                                .field("winStart", Types.STRING())
//                                .rowtime(new Rowtime()
//                                        .timestampsFromField("winStart")
//                                        .watermarksPeriodicBounded(60000)
//                                )
                                .field("winEnd", Types.STRING())
                                .field("pv", Types.LONG())
                )
                .inAppendMode()
                .registerTableSink("KafkaRegionPvSinkTable");

        regionPv.insertInto("KafkaRegionPvSinkTable");

    }

    public void testElasticsearchConnector() throws Exception {
        Table regionPv = tEnv.scan("regionPv");

        Elasticsearch elasticsearch = new Elasticsearch()
                .version("6")                      // required: valid connector versions are "6"
                .host("localhost", 9201, "http")   // required: one or more Elasticsearch hosts to connect to
                .index("flink_sql_region_pv")                  // required: Elasticsearch index
                .documentType("RegionPv")              // required: Elasticsearch document type
                .keyDelimiter("_")        // optional: delimiter for composite keys ("_" by default)
                //   e.g., "$" would result in IDs "KEY1$KEY2$KEY3"
                .keyNullLiteral("null")    // optional: representation for null fields in keys ("null" by default)

                // optional: failure handling strategy in case a request to Elasticsearch fails (fail by default)
                .failureHandlerFail()          // optional: throws an exception if a request fails and causes a job
                // failure
//                .failureHandlerIgnore()        //   or ignores failures and drops the request
//                .failureHandlerRetryRejected() //   or re-adds requests that have failed due to queue capacity
//                saturation
//                .failureHandlerCustom(...)     //   or custom failure handling with a ActionRequestFailureHandler
//                subclass

                // optional: configure how to buffer elements before sending them in bulk to the cluster for efficiency
//                .disableFlushOnCheckpoint()    // optional: disables flushing on checkpoint (see notes below!)
//                .bulkFlushMaxActions(42)       // optional: maximum number of actions to buffer for each bulk request
//                .bulkFlushMaxSize("42 mb")     // optional: maximum size of buffered actions in bytes per bulk request
                //   (only MB granularity is supported)
//                .bulkFlushInterval(60000L)     // optional: bulk flush interval (in milliseconds)

//                .bulkFlushBackoffConstant()    // optional: use a constant backoff type
//                .bulkFlushBackoffExponential() //   or use an exponential backoff type
//                .bulkFlushBackoffMaxRetries(3) // optional: maximum number of retries
//                .bulkFlushBackoffDelay(30000L) // optional: delay between each backoff attempt (in milliseconds)

                // optional: connection properties to be used during REST communication to Elasticsearch
                .connectionMaxRetryTimeout(5000); // optional: maximum timeout (in milliseconds) between retries
//                .connectionPathPrefix("/v1");// optional: prefix string to be added to every REST communication

        Json json = new Json()
                .failOnMissingField(true)
                .schema(TableSchema
                        .builder()
                        .field("region", Types.STRING())
                        .field("winStart", Types.SQL_TIMESTAMP())
                        .field("winEnd", Types.SQL_TIMESTAMP())
                        .field("pv", Types.LONG())
                        .build()
                        .toRowType()
                )
                .jsonSchema(
                        RichRowTypeInfo
                                .bulider()
                                .field("region", Types.STRING(), "1")
                                .field("winStart", Types.SQL_TIMESTAMP(), "2")
                                .field("winEnd", Types.SQL_TIMESTAMP(), "3")
                                .field("pv", Types.LONG(), "4")
                                .build()
                                .toJsonSchema()
                );

        tEnv
                .connect(elasticsearch)
                .withFormat(json)
                .withSchema(new Schema()
                        .field("region", Types.STRING())
                        .field("winStart", Types.SQL_TIMESTAMP())
                        .field("winEnd", Types.SQL_TIMESTAMP())
                        .field("pv", Types.LONG())
                )
                .inUpsertMode()
                .registerTableSink("EsRegionPvSinkTable");

        regionPv.insertInto("EsRegionPvSinkTable");

    }

    private void testJDBCAppendConnector() {
        Table regionPv = tEnv.scan("regionPv");

        TypeInformation[] types = {
                Types.STRING(), Types.SQL_TIMESTAMP(),
                Types.SQL_TIMESTAMP(), Types.LONG()
        };

        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("org.gjt.mm.mysql.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/flink")
                .setUsername("root")
                .setPassword("root")
                .setQuery("INSERT INTO flink_region_pv (region, winStart, winEnd, pv) VALUES (?, ?, ?, ?)")
                .setParameterTypes(types)
                .build();

        tEnv.registerTableSink(
                "FlinkJdbcSinkTable",
                // specify table schema
                new String[]{"region", "winStart", "winEnd", "pv"},
                types,
                sink);

        regionPv.insertInto("FlinkJdbcSinkTable");
    }

    /**
     * UDF
     * 用户想自己编写一个字符串联接的UDF，我们只需要实现ScalarFunction#eval()方法即可
     */
    @Test
    public void testUDF() {
        tEnv.registerFunction("MyConCat", new MyConCat());
        Table table = tEnv.sqlQuery(
                "SELECT  \n" +
                "    MyConCat(c_name, c_desc) \n" +
                "FROM customer_tab\n"
        );

        tEnv.toRetractStream(table, Row.class).print("");
    }

    /**
     * UDTF
     * 用户想自己编写一个字符串切分的UDTF，我们只需要实现TableFunction#eval()方法即可
     * SELECT c, s FROM MyTable, LATERAL TABLE(mySplit(c)) AS T(s)
     */
    @Test
    public void testUDTF() {
        tEnv.registerFunction("MySplit", new MySplit());
        Table table = tEnv.sqlQuery(
                "SELECT c_desc, s FROM customer_tab, LATERAL TABLE(MySplit(c_desc)) AS T(s)"
        );

        tEnv.toRetractStream(table, Row.class).print("");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegionPv {
        private String region;
        private Timestamp winStart;
        private Timestamp winEnd;
        private Long pv;
    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class NewRegionPv {
//        private String region;
//        private LocalDateTime winStart;
//        private LocalDateTime winEnd;
//        private Long pv;
//    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NewRegionPv {
        private String region;
        private String winStart;
        private String winEnd;
        private Long pv;
    }


    /**
     * UDF - User-Defined Scalar Function
     * UDTF - User-Defined Table Function
     * UDAF - User-Defined Aggregate Funciton
     * <p>
     * UDX	INPUT	OUTPUT	INPUT:OUTPUT
     * UDF	单行中的N(N>=0)列	单行中的1列	1:1
     * UDTF	单行中的N(N>=0)列	M(M>=0)行	1:N(N>=0)
     * UDAF	M(M>=0)行中的每行的N(N>=0)列	单行中的1列	M：1(M>=0)
     */

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RegionFormatPv {
        private String region;
        private String winStart;
        private String winEnd;
        private Long pv;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccessCountSum {
        private Timestamp winStart;
        private Timestamp winEnd;
        private Integer accessCount;
    }

    public static class MyConCat extends ScalarFunction {

        public String eval(String... args) {
            StringBuilder sb = new StringBuilder();
            for (String s : args) {
                sb.append(s);
            }

            return sb.toString();
        }
    }

    public static class MySplit extends TableFunction<String> {

        public void eval(String str) {
            for (String s : str.split(" ")) {
                collect(s);   // use collect(...) to emit an output row
            }
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
    public static class ItemData {
        private Long onSellTime;
        private Integer price;
        private String itemID;
        private String itemType;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MaxPriceItem {
        private Timestamp onSellTime;
        private Integer price;
        private Integer maxPrice;
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
