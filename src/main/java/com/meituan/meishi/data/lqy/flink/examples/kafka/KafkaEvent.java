package com.meituan.meishi.data.lqy.flink.examples.kafka;

/**
 * The event type used in the {@link Kafka010Example}.
 *
 * <p>This is a Java POJO, which Flink recognizes and will allow "by-name" field referencing
 * when keying a {@link org.apache.flink.streaming.api.datastream.DataStream} of such a type.
 * For a demonstration of this, see the code in {@link Kafka010Example}.
 */
public class KafkaEvent {

	private String word;
	private int frequency;
	private long timestamp;

	public KafkaEvent() {}

	public KafkaEvent(String word, int frequency, long timestamp) {
		this.word = word;
		this.frequency = frequency;
		this.timestamp = timestamp;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public static KafkaEvent fromString(String eventStr) {
		String[] split = eventStr.split(",");
		return new KafkaEvent(
				split[0],
				Integer.valueOf(split[1].replaceAll("\\D", "")),
				Long.valueOf(split[2].replaceAll("\\D", ""))
		);
	}

	@Override
	public String toString() {
		return word + "," + frequency + "," + timestamp;
	}
}
