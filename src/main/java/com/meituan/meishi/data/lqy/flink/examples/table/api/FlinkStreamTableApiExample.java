package com.meituan.meishi.data.lqy.flink.examples.table.api;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkStreamTableApiExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> source = env.fromElements(
                Row.of("a1", 1, "c1"),
                Row.of("A1", 2, "c2"),
                Row.of("a3", 3, "c3"),
                Row.of("A4", 4, "c4"),
                Row.of("a3", 5, "c5")
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row element) {
                return System.currentTimeMillis();
            }
        });

        Table orders = tEnv.fromDataStream(source, "a, b, c, d.rowtime");
        Table result = orders
                .filter("a.isNotNull && b.isNotNull && c.isNotNull")
                .select("a.lowerCase() AS a, b, d")
                .window(Tumble.over("1.hour").on("d").as("hourlyWindow"))
//                .window(Tumble.over("1.millis").on("rowtime").as("millisWindow"))
                .groupBy("hourlyWindow, a")
                .select("a, hourlyWindow.end As mill, b.avg AS avgBillingAmount");

        tEnv.toAppendStream(result, Row.class).print("");

        env.execute("Flink Stream Table Api Example");
    }
}
