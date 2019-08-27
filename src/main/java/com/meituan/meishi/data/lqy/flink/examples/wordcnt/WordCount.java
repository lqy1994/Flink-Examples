package com.meituan.meishi.data.lqy.flink.examples.wordcnt;

import com.meituan.meishi.data.lqy.flink.examples.wordcnt.util.WordCountData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class WordCount {

    public static final String DIR = "/Users/liqingyong/Works/Learn/Flink/Proj/Flink-Examples/src/main/resources/wordcnt/";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, String> map = new HashMap<>();

//        map.put("input", DIR + "text.txt");
        map.put("output", DIR + "/wordcnt.txt");
        ParameterTool params = ParameterTool.fromMap(map);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        SingleOutputStreamOperator<Tuple2<String, Integer>> counts =  text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] tokens = value.toLowerCase().split("\\W+");

                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }
        });

        counts.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        env.execute("Streaming WordCount");
    }
}
