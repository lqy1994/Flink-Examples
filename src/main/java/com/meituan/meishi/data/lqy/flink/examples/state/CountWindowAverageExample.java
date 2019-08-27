package com.meituan.meishi.data.lqy.flink.examples.state;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class CountWindowAverageExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Long, Long>> result = env
                .fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {

                    private transient ValueState<Tuple2<Long, Long>> sum;

                    @Override
                    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
                        Tuple2<Long, Long> current = sum.value();
                        if (current == null) {
                            current = Tuple2.of(0L, 0L);
                        }

                        current.f0 += 1;
                        current.f1 += input.f1;

                        sum.update(current);

                        if (current.f0 >= 2) {
                            out.collect(Tuple2.of(input.f0, current.f1 / current.f0));
                            sum.clear();
                        }
                    }

                    @Override
                    public void open(Configuration config) throws Exception {

                        StateTtlConfig ttlConfig = StateTtlConfig
                                .newBuilder(Time.seconds(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                .cleanupFullSnapshot()
                                .build();

                        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                                new ValueStateDescriptor<>(
                                        "average",
                                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                                        })
                                );
                        descriptor.enableTimeToLive(ttlConfig);
                        descriptor.setQueryable("query-name");
                        sum = getRuntimeContext().getState(descriptor);
                    }
                });

        result.map(new MapFunction<Tuple2<Long, Long>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<Long, Long> value) throws Exception {
                return Tuple2.of(String.valueOf(value.f0), Math.toIntExact(value.f1));
            }
        }).addSink(new BufferingSink(1));

        env.execute(CountWindowAverageExample.class.getSimpleName());

    }

    public static class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

        private final int threshold;

        private transient ListState<Tuple2<String, Integer>> checkpointedState;

        private List<Tuple2<String, Integer>> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, SinkFunction.Context context) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (int i = 0; i < bufferedElements.size(); i++) {
                    System.out.println("Sink Element: " + bufferedElements.get(i));
                }
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (Tuple2<String, Integer> element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                    new ListStateDescriptor<>(
                            "buffered-element",
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                    );

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (Tuple2<String, Integer> element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }

        }
    }
}
