//package com.meituan.meishi.data.lqy.flink.examples.table;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
//import org.apache.flink.streaming.connectors.kafka.KafkaTableSourceSinkFactory;
//import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
//import org.apache.flink.table.api.*;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.table.sinks.CsvTableSink;
//import org.apache.flink.table.sinks.TableSink;
//import org.apache.flink.table.sources.DefinedProctimeAttribute;
//import org.apache.flink.table.sources.StreamTableSource;
//import org.apache.flink.types.Row;
//
//import javax.annotation.Nullable;
//import java.util.Arrays;
//import java.util.Optional;
//import java.util.Properties;
//
//public class TableTimeWindowExample {
//
//    public static final String PATH = "/Users/liqingyong/Works/Learn/Flink/Proj/Flink-Examples/src/main/resources/table/";
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tEnv = StreamTableEnvironment.getTableEnvironment(env);
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        //1.
//        DataStream<Tuple2<String, String>> stream = env.fromCollection(
//                Arrays.asList(
//                        Tuple2.of("Liqingyong", "System RD"),
//                        Tuple2.of("Liuyuming", "Data RD"),
//                        Tuple2.of("xiarui", "Data RD"),
//                        Tuple2.of("xuyang", "System RD")
//                )
//        );
//
//        SingleOutputStreamOperator<Tuple2<String, String>> filter = stream
//                .filter((FilterFunction<Tuple2<String, String>>) value -> value.f0.startsWith("Li"));
//
//
//        Table table = tEnv.fromDataStream(filter, "UserName, Data, UserActionTime.procTime");
//
//        WindowedTable windowedTable = table
//                .window(Tumble
//                        .over("10.minutes")
//                        .on("UserActionTime.procTime")
//                        .as("UserActionWindow")
//                );
//
//        CsvTableSink sink1 = new CsvTableSink(PATH + "table1.csv", " ");
//        tEnv.registerTableSink(
//                "OutTable1",
//                new String[]{"UserName", "Data", "UserActionWindow"},
//                new TypeInformation[]{Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()},
//                sink1
//        );
//
//        windowedTable.table().insertInto("OutTable1");
//
//        //2.
//        tEnv.registerTableSource("UserTable", new UserActionSource());
////
//        WindowedTable windowedTable1 = tEnv
//                .scan("UserTable")
//                .window(Tumble.over("10.minutes").on("UserActionTime").as("UserActionWindow"));
//
//        TableSchema schema = TableSchema.builder()
//                .field("UserName", Types.STRING())
//                .field("Data", Types.STRING())
//                .field("UserActionWindow", Types.SQL_TIMESTAMP())
//                .build();
//
//        Properties properties = new Properties();
//        properties.setProperty("zookeeper.connect", "localhost:2181");
//        properties.setProperty("group.id", "consume-kafka");
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//
//        KafkaTableSink sink2 =
//                new KafkaTableSink(
//                        schema,
//                        "table-time-window-topic",
//                        properties,
//                        Optional.of(new FlinkFixedPartitioner<>()),
//                        new TypeInformationSerializationSchema<>(schema.toRowType(), env.getConfig())
//                );
//
//        tEnv.registerTableSink("OutTable2", sink2);
//        windowedTable1.table().insertInto("OutTable2");
//
//        env.execute("Table Time Window Example");
//
//    }
//
//    private static class UserActionSource implements StreamTableSource<Row>, DefinedProctimeAttribute {
//
//        @Nullable
//        @Override
//        public String getProctimeAttribute() {
//            return "UserActionTime";
//        }
//
//        @Override
//        public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
//            return env.fromCollection(
//                    Arrays.asList(
//                            Row.of("Liqingyong", "System RD"),
//                            Row.of("Liuyuming", "Data RD"),
//                            Row.of("xiarui", "Data RD"),
//                            Row.of("xuyang", "System RD")
//                    )
//            );
//        }
//
//        @Override
//        public TypeInformation<Row> getReturnType() {
//            String[] names = new String[]{"UserName", "Data"};
//            TypeInformation[] types = new TypeInformation[]{Types.STRING(), Types.STRING()};
//            return Types.ROW(names, types);
//        }
//
//        @Override
//        public TableSchema getTableSchema() {
//            String[] names = new String[]{"UserName", "Data", "UserActionTime"};
//            TypeInformation[] types = new TypeInformation[]{Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()};
//            return new TableSchema(names, types);
//        }
//
//    }
//}
