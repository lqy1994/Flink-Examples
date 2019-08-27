//package com.meituan.meishi.data.lqy.flink.examples.elasticsearch;
//
//import com.google.common.collect.Lists;
//import com.google.gson.Gson;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//import org.elasticsearch.client.transport.TransportClient;
//
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class ESFlinkDemo {
//
//    public static  Gson gson = new Gson();
//
//    public static void main(String[] args) throws Exception {
//
//        Map<String, String> config = new HashMap<>();
//        config.put("cluster.name", "elasticsearch");
//        config.put("bulk.flush.max.actions", "1");
//
//        List<InetSocketAddress> transportAddresses = new ArrayList<>();
//        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
//
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        List<Table> list = Lists.newArrayList();
//        for (int i = 1; i <= 1000; i++) {
//            list.add(newTable(i));
//        }
//
//        DataStreamSource<Table> source = env.fromCollection(list);
//
//       source.map(new MapFunction<Table, String>() {
//            @Override
//            public String map(Table table) throws Exception {
//                return gson.toJson(table);
//            }
//        }).addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
//
//            @Override
//           public void process(String element, RuntimeContext runtimeContext, RequestIndexer indexer) {
//                   indexer.add(createIndexRequest(element));
//           }
//
//           private IndexRequest createIndexRequest(String element) {
//               Map<String, String> json = new HashMap<>();
//               json.put("data", element);
//
//               return Requests.indexRequest()
//                       .index("table-index")
//                       .type("Table")
//                       .source(json);
//           }
//
//       }));
//
//       env.execute("ES-Flink -Table");
//
//    }
//
//    private static Table newTable(int i) {
//        return new Table(i, "App_Table_" + i, "表格_" + i,
//                "liqingyong02", "测试表格" + i);
//    }
//}
