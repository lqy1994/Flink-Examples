package com.meituan.meishi.data.lqy.flink.examples.process.function;

import lombok.Data;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class CountWithTimeoutFunctionExample {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountWithTimeoutFunctionExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<Tuple2<String, String>> source =
                env.addSource(new RichParallelSourceFunction<Tuple2<String, String>>() {

                    @Override
                    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
                        for (int i = 0; i < 10; i++) {
                            int k = i + 1;
                            ctx.collectWithTimestamp(Tuple2.of("key_" + k, "value_" + k), k);
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Long>> result =
                source.keyBy(0)
                .process(new CountWithTimeoutFunction());

        result.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                System.out.println("SINK: " + value);
            }
        }).setParallelism(1);

        env.execute("Count With Timeout Function Example");

    }

    @Data
    static class CountWithTimestamp implements Serializable {

        private static final long serialVersionUID = 5205854652243084230L;

        private String key;
        private long count;
        private Long lastModified;
    }

    /**
     * The implementation of the ProcessFunction that maintains the count and timeouts
     */
    static class CountWithTimeoutFunction
            extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, Long>> {

        /**
         * The state that is maintained by this process function
         */
        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void processElement(
                Tuple2<String, String> value,
                Context ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // retrieve the current count
            CountWithTimestamp current = state.value();
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            // update the state's count
            current.count++;

            // set the state's timestamp to the record's assigned event time timestamp
            current.lastModified = ctx.timestamp();

            // write the state back
            state.update(current);

            // schedule the next timer 60 seconds from the current event time
            ctx.timerService().registerEventTimeTimer(current.lastModified + 1000);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple2<String, Long>> out) throws Exception {

            // get the state for the key that scheduled the timer
            CountWithTimestamp result = state.value();

            // check if this is an outdated timer or the latest timer
            if (timestamp == result.lastModified + 1000) {
                // emit the state on timeout
                out.collect(new Tuple2<String, Long>(result.key, result.count));
            }
        }
    }

}
