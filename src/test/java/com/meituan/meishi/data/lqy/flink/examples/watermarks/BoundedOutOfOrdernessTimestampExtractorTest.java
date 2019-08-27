package com.meituan.meishi.data.lqy.flink.examples.watermarks;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BoundedOutOfOrdernessTimestampExtractorTest extends AbstractTestBase {

    @Test
    public void testInitializationAndRuntime() {
        Time maxAllowedLateness = Time.milliseconds(10);
        BoundedOutOfOrdernessTimestampExtractor<Long> extractor =
                new LongExtractor(maxAllowedLateness);

        assertEquals(maxAllowedLateness.toMilliseconds(), extractor.getMaxOutOfOrdernessInMillis());

        runValidTests(extractor);
    }

    @Test
    public void testInitialFinalAndWatermarkUnderflow() {
        BoundedOutOfOrdernessTimestampExtractor<Long> extractor = new LongExtractor(Time.milliseconds(10L));
        assertEquals(Long.MIN_VALUE, extractor.getCurrentWatermark().getTimestamp());

        extractor.extractTimestamp(Long.MIN_VALUE, -1L);

        extractor.extractTimestamp(Long.MIN_VALUE + 2, -1);
        assertEquals(Long.MIN_VALUE, extractor.getCurrentWatermark().getTimestamp());

        extractor.extractTimestamp(Long.MAX_VALUE, -1L);
        assertEquals(Long.MAX_VALUE - 10, extractor.getCurrentWatermark().getTimestamp());

    }


    private void runValidTests(BoundedOutOfOrdernessTimestampExtractor<Long> extractor) {
        assertEquals(new Watermark(Long.MIN_VALUE), extractor.getCurrentWatermark());

        assertEquals(13L, extractor.extractTimestamp(13L, 0L));
        assertEquals(13L, extractor.extractTimestamp(13L, 0L));
        assertEquals(14L, extractor.extractTimestamp(14L, 0L));
        assertEquals(20L, extractor.extractTimestamp(20L, 0L));

        assertEquals(new Watermark(10L), extractor.getCurrentWatermark());

        assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        assertEquals(20L, extractor.extractTimestamp(20L, 0L));
        assertEquals(500L, extractor.extractTimestamp(500L, 0L));

        assertEquals(new Watermark(490L), extractor.getCurrentWatermark());

        assertEquals(Long.MAX_VALUE - 1, extractor.extractTimestamp(Long.MAX_VALUE - 1, 0L));
        assertEquals(new Watermark(Long.MAX_VALUE - 11), extractor.getCurrentWatermark());
    }


    private static class LongExtractor extends BoundedOutOfOrdernessTimestampExtractor<Long> {

        private static final long serialVersionUID = 1L;

        public LongExtractor(Time LongExtractor) {
            super(LongExtractor);
        }

        @Override
        public long extractTimestamp(Long element) {
            return element;
        }
    }

}
