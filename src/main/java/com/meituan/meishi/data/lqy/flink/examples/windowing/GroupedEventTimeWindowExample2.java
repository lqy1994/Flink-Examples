package com.meituan.meishi.data.lqy.flink.examples.windowing;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class GroupedEventTimeWindowExample2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);

        DataStream<Tuple2<Long, Long>> source = env
                .addSource(new SourceFunction<Tuple2<Long, Long>>() {

                    private volatile boolean running = true;

                    @Override
                    public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
                        final long startTime = System.currentTimeMillis();

                        final long numElements = 20000000;
                        final long numKeys = 10000;
                        long val = 1L;
                        long count = 0L;

                        while (running && count < numElements) {
                            count++;
                            ctx.collect(new Tuple2<>(val++, 1L));

                            if (val > numKeys) {
                                val = 1L;
                            }
                        }

                        final long endTime = System.currentTimeMillis();
                        System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                });

        SingleOutputStreamOperator<Tuple2<Long, Double>> output = source
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, Long>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple2<Long, Long> element) {
                        return System.currentTimeMillis();
                    }
                })
                .keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {
                    @Override
                    public Long getKey(Tuple2<Long, Long> value) throws Exception {
                        return (Long) value.getField(0);
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.milliseconds(2500), Time.milliseconds(500)))
                .aggregate(new AggregateFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Double>() {

                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return Tuple2.of(0L, 0L);
                    }

                    @Override
                    public Tuple2<Long, Long> add(Tuple2<Long, Long> value, Tuple2<Long, Long> accumulator) {
                        return Tuple2.of(accumulator.f0 + value.f1, accumulator.f1 + 1L);
                    }

                    @Override
                    public Double getResult(Tuple2<Long, Long> accumulator) {
                        return ((double) accumulator.f0) / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                }, new ProcessWindowFunction<Double, Tuple2<Long, Double>, Long, TimeWindow>() {

                    @Override
                    public void process(Long key, Context context,
                                        Iterable<Double> averages,
                                        Collector<Tuple2<Long, Double>> out) throws Exception {
                        Double average = averages.iterator().next();
                        out.collect(Tuple2.of(key, average));
                    }
                });

        Iterator<Tuple2<Long, Double>> collect = DataStreamUtils.collect(output);

//        List<Tuple2<Long, Double>> res = new ArrayList<>();
//        collect.forEachRemaining(res::add);

        List<Tuple2<Long, Double>> res = Lists.newArrayList(collect);
        res
                .stream()
                .sorted(Comparator
                        .comparing((Function<Tuple2<Long, Double>, Long>) tuple -> tuple.f0)
                        .thenComparing(tuple -> tuple.f1)
                ).forEach(System.out::println);

        output.addSink(new SinkFunction<Tuple2<Long, Double>>() {
            @Override
            public void invoke(Tuple2<Long, Double> value, Context context) throws Exception {
            }
        });

        env.execute("Group Event Aggregate Window Example");
    }
}
