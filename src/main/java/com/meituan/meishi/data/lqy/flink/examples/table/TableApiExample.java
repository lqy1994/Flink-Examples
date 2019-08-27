package com.meituan.meishi.data.lqy.flink.examples.table;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;

public class TableApiExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        tEnv.registerTable("table1", );
        tEnv.registerTableSource(
                "table2",
                new CsvTableSource(
                        "path/to/table2",
                        new String[]{"a", "b", "c"},
                        new TypeInformation[]{Types.STRING, Types.DOUBLE}
                )
        );
//        tEnv.registerExternalCatalog("table3", );

//        tEnv.registerTableSink("outputTable", );

//        tEnv.fromDataStream()


        // create a Table from a Table API query
        Table tapiResult = tEnv.scan("table1").select("field1");
        // create a Table from a SQL query
        Table sqlResult = tEnv.sqlQuery("SELECT ... FROM table2 ... ");

        sqlResult.insertInto("outputTable");


        tEnv.registerTableSink(
                "tableName1",
                new String[]{"a", "b", "c"},
                new TypeInformation[]{Types.INT, Types.STRING, Types.LONG},
                new CsvTableSink("/path/to/table/sink", ",")
        );


        Table table2 = tEnv.scan("table2");
        table2.filter("a === 'a'")
                .groupBy("b")
                .select("c");


        Table query = tEnv.sqlQuery("SELECT *****");

        DataStream<String> stream = env.fromCollection(Lists.newArrayList("a", "b"));
        tEnv.registerDataStream("stream1", stream, "field");
        tEnv.fromDataStream(stream, "a");


        tEnv.toAppendStream(query, Row.class);
        DataStream<Tuple2<String, Integer>> dsTuple = tEnv.toAppendStream(
                table2,
                new TupleTypeInfo<>(
                        org.apache.flink.table.api.Types.STRING(),
                        org.apache.flink.table.api.Types.INT()
                )
        );
//        tEnv.toRetractStream()


//// convert DataStream into Table with default field names "f0" and "f1"
//        Table table = tableEnv.fromDataStream(stream);
//
//// convert DataStream into Table with field "f1" only
//        Table table = tableEnv.fromDataStream(stream, "f1");
//
//// convert DataStream into Table with swapped fields
//        Table table = tableEnv.fromDataStream(stream, "f1, f0");
//
//// convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
//        Table table = tableEnv.fromDataStream(stream, "f1 as myInt, f0 as myLong");

        env.execute("Table Api Example");

    }
}
