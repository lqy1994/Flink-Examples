package com.meituan.meishi.data.lqy.flink.examples.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;

import java.sql.Timestamp;

public class TemporalTableJoinExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Timestamp, Integer, String>> orderSrc = env
                .fromElements(
                        Tuple3.of(new Timestamp(2L), 2, "Euro"),
                        Tuple3.of(new Timestamp(3L), 1, "US Dollar"),
                        Tuple3.of(new Timestamp(4L), 50, "Yen"),
                        Tuple3.of(new Timestamp(5L), 3, "Euro")
//                        Tuple3.of(new Timestamp(6L), 5, "US Dollar")
                )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Timestamp, Integer, String>>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<Timestamp, Integer, String> element) {
                        return element.f0.getTime();
                    }
                });

        Table orders = tEnv.fromDataStream(orderSrc, "rowtime.rowtime, amount, currency");
        tEnv.registerTable("Orders", orders);

        DataStream<Tuple3<Timestamp, String, Integer>> rates = env
                .fromElements(
                        Tuple3.of(new Timestamp(1L), "US Dollar", 102),
                        Tuple3.of(new Timestamp(1L), "Euro", 114),
                        Tuple3.of(new Timestamp(1L), "Yen", 1),
                        Tuple3.of(new Timestamp(5L), "Euro", 116),
                        Tuple3.of(new Timestamp(7L), "Euro", 117)
//                        Tuple3.of(new Timestamp(8L), "Pounds", 108)
                )
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Timestamp, String, Integer>>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(Tuple3<Timestamp, String, Integer> element) {
                        return element.f0.getTime();
                    }
                });

        Table ratesHistory = tEnv.fromDataStream(rates, "rowtime.rowtime, currency, rate");
        tEnv.registerTable("RatesHistory", ratesHistory);

        TemporalTableFunction ratesFunc = ratesHistory.createTemporalTableFunction("rowtime", "currency");
        tEnv.registerFunction("Rates", ratesFunc);

        Table query1 = tEnv
                .sqlQuery(
                        "SELECT\n" +
                        "  SUM(o.amount * r.rate) AS amount\n" +
                        "FROM Orders AS o,\n" +
                        "  RatesHistory AS r\n" +
                        "WHERE r.currency = o.currency\n" +
                        "AND r.rowtime = (\n" +
                        "  SELECT CAST(MAX(rowtime) AS TIMESTAMP) rowtime\n" +
                        "  FROM RatesHistory AS r2\n" +
                        "  WHERE r2.currency = o.currency\n" +
                        "  AND r2.rowtime <= o.rowtime)\n"
                );

        query1.printSchema();

        DataStream<Row> stream = tEnv.toAppendStream(query1, TableSchema.builder().field("amount", Types.INT).build().toRowType());
        stream.print();

//        TableSchema schema1 = TableSchema.builder()
//                .field("amount", Types.INT)
//                .build();
//
//        DataStream<Amount> map1 = tEnv
//                .toRetractStream(query1, schema1.toRowType())
//                .map(new MapFunction<Tuple2<Boolean, Row>, Amount>() {
//                    @Override
//                    public Amount map(Tuple2<Boolean, Row> value) throws Exception {
//                        return new Amount("",
//                                Integer.valueOf(String.valueOf(value.f1.getField(0)))
//                        );
//                    }
//                });
//
//        map1.print();

        System.out.println("=================================================");

        Table query2 = tEnv
                .sqlQuery(
                        "SELECT\n" +
                                "   o.currency,\n" +
                                "   o.amount * r.rate AS amount\n" +
                                "FROM\n" +
                                "   Orders AS o,\n" +
                                "   LATERAL TABLE (Rates(o.rowtime)) AS r\n" +
                                "WHERE r.currency = o.currency"
                );

        TableSchema schema2 = TableSchema.builder()
                .field("currency", Types.STRING)
                .field("amount", Types.INT)
                .build();

        DataStream<Amount> map2 = tEnv
                .toRetractStream(query2, schema2.toRowType())
                .map(new MapFunction<Tuple2<Boolean, Row>, Amount>() {
                    @Override
                    public Amount map(Tuple2<Boolean, Row> value) throws Exception {
                        return new Amount(
                                String.valueOf(value.f1.getField(0)),
                                Integer.valueOf(String.valueOf(value.f1.getField(1)))
                        );
                    }
                });

        map2.print();

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Amount {
        private String currency;
        private Integer amount;
    }
}


