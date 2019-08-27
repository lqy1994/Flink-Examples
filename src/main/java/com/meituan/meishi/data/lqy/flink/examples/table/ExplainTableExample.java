package com.meituan.meishi.data.lqy.flink.examples.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class ExplainTableExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<Integer, String>> stream1 = env.fromElements(Tuple2.of(1, "Hello"), Tuple2.of(2, "World"), Tuple2.of(3, "Flink"));
        DataStreamSource<Tuple2<Integer, String>> stream2 = env.fromElements(Tuple2.of(1, "I"), Tuple2.of(2, "Love"), Tuple2.of(3, "Java"));

        Table table1 = tableEnv.fromDataStream(stream1, "count, word");
        Table table2 = tableEnv.fromDataStream(stream2, "count, word");


        Table table = table1
                .where("LIKE(word, 'F%')")
                .unionAll(table2);

        String explain = tableEnv.explain(table);
        System.out.println(explain);

        env.execute("Explain Table Example");
    }
}
