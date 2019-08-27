package com.meituan.meishi.data.lqy.flink.examples.table;

import com.google.common.collect.Lists;
import com.meituan.meishi.data.lqy.flink.examples.utils.GsonHolder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.types.Row;

import java.util.*;

public class TableEventTimeWindowExample {

    static final TableSchema schema = TableSchema.builder()
            .field("Username", Types.STRING())
            .field("HomeTown", Types.STRING())
            .field("UserActionTime", Types.SQL_TIMESTAMP())
            .build();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1. During DataStream-to-Table Conversion
        DataStream<Tuple3<String, String, Long>> stream = env
                .fromCollection(
                        Arrays.asList(
                            Tuple3.of("liqingyong", "shandong", 1L),
                            Tuple3.of("liuyuming", "hebei", 1L),
                            Tuple3.of("xiarui", "henan", 2L),
                            Tuple3.of("xuyang", "shandong", 3L),
                            Tuple3.of("huangweilun", "beijing", 2L),
                            Tuple3.of("lixiaoying", "gansu", 4L),
                            Tuple3.of("zhongjie", "shanxi", 3L)
                        )
                ).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple3<String, String, Long> element) {
                        return System.currentTimeMillis() + element.f2;
                    }
                });

        Table table = tEnv.fromDataStream(stream, "UserName, HomeTown, UserActionTime.rowtime");
        GroupWindowedTable windowedTable = table.window(Slide.over("10.seconds").every("5.seconds").on("UserActionTime").as("userActionWindow"));


        TableSchema tableSchema = TableSchema.builder()
                .field("UserName", Types.STRING())
                .field("HomeTown", Types.STRING())
                .field("UserActionWindow", Types.SQL_TIMESTAMP())
                .build();

//        Table table1 = windowedTable.table();
//        DataStream<Row> sinkStream = tEnv.toAppendStream(table1, tableSchema.toRowType());
//        Iterator<Row> collect = DataStreamUtils.collect(sinkStream);
//        ArrayList<Row> list = Lists.newArrayList(collect);
//        System.out.println(GsonHolder.GSON.toJson(list));

        //2.
//        tEnv.registerTableSource("UserActionSource", new UserActionSource());
//        GroupWindowedTable windowedTable1 = tEnv
//                .scan("UserActionSource")
//                .window(Slide.over("10.mills").every("5.mills").on("UserActionTime").as("UserActionWindow"));
//
//        TableSchema tableSchema1 = TableSchema.builder()
//                .field("Username", Types.STRING())
//                .field("HomeTown", Types.STRING())
//                .field("UserActionTime", Types.SQL_TIMESTAMP())
//                .build();
//
//        tEnv.registerTableSink(
//                "UserActionTableSink",
//                tableSchema1.getFieldNames(),
//                tableSchema1.getFieldTypes(),
//                new CsvTableSink("/Users/liqingyong/Works/Learn/Flink/Proj/Flink-Examples/src/main/resources/table.table3", "\t")
//        );
//
//        Table winTable = windowedTable1.table();
////        winTable.insertInto("UserActionTableSink");
//
//
//        Table select = winTable.select("Username, HomeTown, UserActionTime");
//        select.insertInto("UserActionTableSink");
//
//
////        DataStream<Row> sinkStream1 = tEnv.toAppendStream(winTable, tableSchema1.toRowType());
////        ArrayList<Row> list1 = Lists.newArrayList(DataStreamUtils.collect(sinkStream1));
////        System.out.println(GsonHolder.GSON.toJson(list1));

        env.execute("Table Event Time Window Example1");
    }

    public static class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

        @Override
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
            RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor(
                    "UserActionTime",
                    new ExistingField("UserActionTime"),
//                    new StreamRecordTimestamp(),
                    new AscendingTimestamps()
            );

            return Collections.singletonList(descriptor);
        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
            DataStreamSource<Tuple2<String, String>> source = env
                    .fromElements(
                            Tuple2.of("liqingyong", "shandong"),
                            Tuple2.of("liuyuming", "hebei"),
                            Tuple2.of("xiarui", "henan"),
                            Tuple2.of("xuyang", "shandong"),
                            Tuple2.of("huangweilun", "beijing"),
                            Tuple2.of("lixiaoying", "gansu"),
                            Tuple2.of("zhongjie", "shanxi")
            );

            return source.map(new MapFunction<Tuple2<String, String>, Row>() {
                @Override
                public Row map(Tuple2<String, String> value) throws Exception {
                    return Row.of(value.f0, value.f1);
                }
            }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {

                @Override
                public long extractAscendingTimestamp(Row element) {
                    return System.currentTimeMillis();
                }
            });
        }

        @Override
        public TypeInformation<Row> getReturnType() {
//            return new RowTypeInfo(schema.getFieldTypes(), schema.getFieldNames());
            return Types.ROW(schema.getFieldNames(), schema.getFieldTypes());
        }

        @Override
        public TableSchema getTableSchema() {
            return schema;
        }

    }
}
