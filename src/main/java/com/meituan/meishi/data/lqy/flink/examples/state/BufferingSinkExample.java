package com.meituan.meishi.data.lqy.flink.examples.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BufferingSinkExample {

    private static final int MAX_MEM_STATE_SIZE = 5;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(5000);
//        env.setStateBackend(new MemoryStateBackend(MAX_MEM_STATE_SIZE, false));
        env.setStateBackend(
                new FsStateBackend(
//                        "hdfs:localhost:50010/flink/checkpoints",
                        "file:///Users/liqingyong/Works/Learn/Flink/Proj/Flink-Examples/src/main/resources/state",
                        false
                )
        );

        DataStreamSource<Tuple2<String, Integer>> source = env.addSource(new CounterSource());

        DataStream<Tuple2<String, Integer>> sum = source
                .keyBy(0)
                .sum(1);

        sum.addSink(new BufferingSink(5));

        env.execute(BufferingSinkExample.class.getSimpleName());
    }

    public static class CounterSource extends RichParallelSourceFunction<Tuple2<String, Integer>>
            implements ListCheckpointed<Tuple2<String, Integer>> {

        private Tuple2<String, Integer> offset = Tuple2.of("key-" + 0, 0);
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();

            while (isRunning) {
                synchronized (lock) {
                    ctx.collect(offset);
                    offset.f1 += 1;
                    offset.f0 = "key-" + offset.f1;
                }

                if (offset.f1 >= 5) {
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }


        @Override
        public List<Tuple2<String, Integer>> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(offset);
        }

        @Override
        public void restoreState(List<Tuple2<String, Integer>> state) throws Exception {
            for (Tuple2<String, Integer> s : state) {
                offset = s;
            }
        }

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
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                            })
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
