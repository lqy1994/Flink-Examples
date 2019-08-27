package com.meituan.meishi.data.lqy.flink.examples.windowing;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class GroupedEventTimeWindowExample {

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

        source
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
//                .trigger(EventTimeTrigger.create())
                .evictor(TimeEvictor.of(Time.of(10, TimeUnit.SECONDS)))
                .trigger(CountTrigger.of(10))
                .process(new ProcessWindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, TimeWindow>() {
                    @Override
                    public void process(Long key, Context context,
                                        Iterable<Tuple2<Long, Long>> elements,
                                        Collector<Tuple2<Long, Long>> out) throws Exception {
                        long sum = 0;
                        for (Tuple2<Long, Long> element : elements) {
                            sum += element.f1;
                        }

                        out.collect(Tuple2.of(key, sum));
                    }
                })
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {
                        System.out.println(value);
                    }
                });

        env.execute(GroupedEventTimeWindowExample.class.getSimpleName());
    }
}
