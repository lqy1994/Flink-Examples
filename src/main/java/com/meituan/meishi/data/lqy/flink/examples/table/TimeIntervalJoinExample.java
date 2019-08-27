package com.meituan.meishi.data.lqy.flink.examples.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimeIntervalJoinExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        long timeMillis = System.currentTimeMillis();

        DataStream<Tuple3<String, String, Long>> source = env
                .fromElements(
                        Tuple3.of("001", "alipay", timeMillis + 1),
                        Tuple3.of("002", "card", timeMillis + 2),
                        Tuple3.of("003", "card", timeMillis + 3),
                        Tuple3.of("004", "weixin", timeMillis + 4),
                        Tuple3.of("005", "alipay", timeMillis + 5)
                )
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                });

        Table payment = tEnv.fromDataStream(source, "orderId, payType, payTime.rowtime");
        tEnv.registerTable("payment", payment);

        DataStream<Tuple3<String, String, Long>> source2 = env
                .fromElements(
                        Tuple3.of("001", "iphone", timeMillis + 10 * 1000),
                        Tuple3.of("002", "mac", timeMillis + 30 * 1000),
                        Tuple3.of("003", "book", timeMillis + 40 * 1000),
                        Tuple3.of("004", "cup", timeMillis + 50 * 1000),
                        Tuple3.of("005", "ipad", timeMillis + 60 * 1000)
                )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element) {
                        return element.f2;
                    }
                });

        Table order = tEnv.fromDataStream(source, "orderId, productName, orderTime.rowtime");
        tEnv.registerTable("orders", order);

        Table query = tEnv
                .sqlQuery(
                    "SELECT o.orderId, " +
                            "o.productName, " +
                            "p.payType, " +
                            "o.orderTime, " +
                            "cast(payTime as timestamp) as payTime " +
                            "FROM orders AS o JOIN payment AS p ON o.orderId = p.orderId AND " +
                            "p.payTime BETWEEN orderTime AND orderTime + INTERVAL '1' MINUTE "
                );

//        TypeInformation[] information = new TypeInformation[] {
//                Types.STRING,
//                Types.STRING,
//                Types.STRING,
//                Types.SQL_TIME,
//                Types.SQL_TIME
//        };

        TableSchema tableSchema = TableSchema.builder()
                .field("orderId", Types.STRING)
                .field("productName", Types.STRING)
                .field("payType", Types.STRING)
                .field("orderTime", Types.SQL_TIMESTAMP)
                .field("payTime", Types.SQL_TIMESTAMP)
                .build();

//        DataStream<Row> stream = tEnv.toAppendStream(query, Row.class);
//        ArrayList<Row> list = Lists.newArrayList(DataStreamUtils.collect(stream));
//        System.out.println(GsonHolder.GSON.toJson(list));

        DataStream<Row> stream = tEnv.toAppendStream(query, tableSchema.toRowType());
//        ArrayList<Row> list = Lists.newArrayList(DataStreamUtils.collect(stream));
//        System.out.println(GsonHolder.GSON.toJson(list));

        stream.map(new MapFunction<Row, JoinResult>() {
            @Override
            public JoinResult map(Row row) throws Exception {
                return new JoinResult(
                        String.valueOf(row.getField(0)),
                        String.valueOf(row.getField(1)),
                        String.valueOf(row.getField(2)),
                        parseTimeStamp(String.valueOf(row.getField(3))),
                        parseTimeStamp(String.valueOf(row.getField(4)))
                );
            }

            private LocalDateTime parseTimeStamp(String field) {
                LocalDateTime time = null;
                try {
                    time = LocalDateTime.parse(field, FORMATTER1);
                } catch (Exception e) {
                }

                if (time != null) {
                    return time;
                }

                try {
                    time = LocalDateTime.parse(field, FORMATTER2);
                } catch (Exception e) {
                }

                return time;
            }
        }).print();

        env.execute("Time Interval Join Example");
    }

    public static final DateTimeFormatter FORMATTER1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS");
    public static final DateTimeFormatter FORMATTER2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class JoinResult {
        private String orderId;
        private String productName;
        private String payType;
        private LocalDateTime orderTime;
        private LocalDateTime payTime;
    }
}
