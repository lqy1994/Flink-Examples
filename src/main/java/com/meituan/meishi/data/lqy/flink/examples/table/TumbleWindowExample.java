package com.meituan.meishi.data.lqy.flink.examples.table;

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TumbleWindowExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

// ingest a DataStream from an external source
        DataStream<Tuple3<Long, String, Integer>> ds = env.fromElements(
                Tuple3.of(1L, "a", 1),
                Tuple3.of(2L, "b", 2),
                Tuple3.of(1L, "c", 3),
                Tuple3.of(3L, "a", 4),
                Tuple3.of(1L, "c", 5),
                Tuple3.of(4L, "d", 6)
        ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, String, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<Long, String, Integer> element) {
                return System.currentTimeMillis() + element.f2;
            }
        });
// register the DataStream as table "Orders"
        tableEnv.registerDataStream("Orders", ds, "user, product, amount, proctime.proctime, rowtime.rowtime");

// compute SUM(amount) per day (in event-time)
        Table result1 = tableEnv.sqlQuery(
                "SELECT user, " +
                "  TUMBLE_START(rowtime, INTERVAL '1' DAY) as wStart,  " +
                "  SUM(amount) AS amt " +
                "FROM Orders " +
                "GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user"
        );

        TableSchema schema1 = TableSchema.builder()
                .field("user", Types.LONG)
                .field("wStart", Types.SQL_TIMESTAMP)
                .field("amt", Types.INT)
                .build();

        DataStream<Tuple2<Boolean, Row>> stream = tableEnv.toRetractStream(result1, schema1.toRowType());
        stream.print().setParallelism(1);


// compute SUM(amount) per day (in processing-time)
        Table result2 = tableEnv.sqlQuery(
                "SELECT user, SUM(amount) FROM Orders GROUP BY TUMBLE(proctime, INTERVAL '1' DAY), user");

// compute every hour the SUM(amount) of the last 24 hours in event-time
        Table result3 = tableEnv.sqlQuery(
                "SELECT product, SUM(amount) FROM Orders GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' DAY), product");

// compute SUM(amount) per session with 12 hour inactivity gap (in event-time)
        Table result4 = tableEnv.sqlQuery(
                "SELECT user, " +
                        "  SESSION_START(rowtime, INTERVAL '12' HOUR) AS sStart, " +
                        "  SESSION_ROWTIME(rowtime, INTERVAL '12' HOUR) AS snd, " +
                        "  SUM(amount) " +
                        "FROM Orders " +
                        "GROUP BY SESSION(rowtime, INTERVAL '12' HOUR), user");


        env.execute(TumbleWindowExample.class.getSimpleName());
    }
}
