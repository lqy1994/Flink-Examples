package com.meituan.meishi.data.lqy.flink.examples.table.api;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkBatchTableApiExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<Row> sourceSet = env.fromElements(
                Row.of("a1", 1, "c1"),
                Row.of("A2", 2, "c2"),
                Row.of("a3", 3, "c3"),
                Row.of("A4", 4, "c4"),
                Row.of("a5", 5, "c5")
        );

        tEnv.registerDataSet("Orders", sourceSet, "a, b, c");

        Table orders = tEnv.scan("Orders");

//        Table count = orders
//                .groupBy("a")
//                .select("a, b.count as cnt");
//
//        tEnv.toDataSet(count, Row.class).print();

        System.out.println("--------------------------------------------------");

        Table result = orders
                .filter("a.isNotNull && b.isNotNull && c.isNotNull")
                .select("a.lowerCase() AS a, b, rowtime")
                .window(Tumble.over("1.hour").on("rowtime").as("hourlyWindow"))
//                .window(Tumble.over("1.millis").on("rowtime").as("millisWindow"))
                .groupBy("hourlyWindow, a")
                .select("a, hourlyWindow.end As mill, b.avg AS avgBillingAmount");

        tEnv.toDataSet(result, Row.class).print();


        env.execute("Flink Batch Table Api Example");
    }
}
