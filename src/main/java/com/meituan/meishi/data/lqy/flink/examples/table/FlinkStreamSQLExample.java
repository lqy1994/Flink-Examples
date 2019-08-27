package com.meituan.meishi.data.lqy.flink.examples.table;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkStreamSQLExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple3<String, String, String>> customerStream = env
                .fromElements(
                        Tuple3.of("c_001", "Kevin", "from JinLin"),
                        Tuple3.of("c_002", "Sunny", "from JinLin"),
                        Tuple3.of("c_003", "JinCheng", "from HeBei")

                );
        Table customer = tEnv.fromDataStream(customerStream, "c_id, c_name, c_desc");


        env.execute("Flink Stream SQL Example");
    }
}
