package com.meituan.meishi.data.lqy.flink.examples.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

public class EventTimeTumbleWindowExample {

    static final String [] namesRes =  {"region", "winStart", "winEnd", "pv"};
    static final TypeInformation [] typesRes = {Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.LONG};

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 6123);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Row> source = env.fromCollection(list)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {

                    private long currentTimestamp = Long.MIN_VALUE;

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTimestamp);
                    }

                    @Override
                    public long extractTimestamp(Row element, long previousElementTimestamp) {
                        long timestamp = (long) element.getField(0);
                        if (timestamp >= currentTimestamp) {
                            currentTimestamp = timestamp;
                            return timestamp;
                        } else {
                            System.out.println("WARNING");
                            return timestamp;
                        }
                    }

                });

        tEnv.registerDataStream("SourceTable", source, "accessTime.rowtime, region, userId");

//        tEnv.registerTableSource("SourceTable", new EventTableSource());

        String sql =
                "SELECT  " +
                "  region, " +
                "  TUMBLE_START(accessTime, INTERVAL '2' MINUTE) AS winStart, " +
                "  TUMBLE_END(accessTime, INTERVAL '2' MINUTE) AS winEnd, " +
                "  COUNT(region) AS pv " +
                " FROM SourceTable " +
                " GROUP BY TUMBLE(accessTime, INTERVAL '2' MINUTE), region ";

        Table result = tEnv.sqlQuery(sql);


        TableSchema tableSchema = new TableSchema(namesRes, typesRes);

        tEnv
                .toRetractStream(result, tableSchema.toRowType())
                .map(new MapFunction<Tuple2<Boolean, Row>, CalRes>() {

                    @Override
                    public CalRes map(Tuple2<Boolean, Row> value) throws Exception {
                        Row row = value.f1;
                        return new CalRes(
                                String.valueOf(row.getField(0)),
                                ((Timestamp) row.getField(1)).getTime(),
                                ((Timestamp) row.getField(2)).getTime(),
                                (long) row.getField(3)
                        );
                    }
                }).print("");

        env.execute("EventTime Tumble Window Example");

    }

    public static final String [] names = {"accessTime", "region", "userId"};
    public static final TypeInformation[] types = {Types.LONG, Types.STRING, Types.STRING};
    public static final TypeInformation[] types1 = {Types.SQL_TIMESTAMP, Types.STRING, Types.STRING};
    public static final TableSchema schema = new TableSchema(names, types1);

//    private static class EventTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {
//
//        @Override
//        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
//            return Collections.singletonList(
//                    new RowtimeAttributeDescriptor(
//                            "accessTime",
//                            new ExistingField("accessTime"),
//                            PreserveWatermarks.INSTANCE()
//                    )
//            );
//        }
//
//        @Override
//        public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
//            return env.addSource(new EventSourceFunction());
//        }
//
//        @Override
//        public TypeInformation<Row> getReturnType() {
////            return new RowTypeInfo(types, names);
//            return org.apache.flink.table.api.Types.ROW(names, types);
//        }
//
//        @Override
//        public TableSchema getTableSchema() {
//            return schema;
//        }
//
//    }

    static final List<Row> list = Arrays.asList(
            Row.of(1510365660000L, "ShangHai", "U0010"),
            Row.of(1510365660000L, "BeiJing", "U1001"),
            Row.of(1510366200000L, "BeiJing", "U2032"),
            Row.of(1510366260000L, "BeiJing", "U1100"),
            Row.of(1510373400000L, "ShangHai", "U0011")
    );

    private static class EventSourceFunction extends RichParallelSourceFunction<Row> {

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {

            for (Row row : list) {
                Long time = (Long) row.getField(0);
                ctx.collectWithTimestamp(row, time);
                ctx.emitWatermark(new Watermark(time));
            }
        }

        @Override
        public void cancel() {
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CalRes {
        private String region;
        private long winStart;
        private long winEnd;
        private long pv;
    }

}
