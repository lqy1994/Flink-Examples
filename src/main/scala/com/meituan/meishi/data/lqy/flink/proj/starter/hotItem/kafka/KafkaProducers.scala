package com.meituan.meishi.data.lqy.flink.proj.starter.hotItem.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaProducers {

  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 定义一个kafka producer
    val producer = new KafkaProducer[String, String](properties)

    val bufferedSource = io.Source.fromFile(
      "/Users/liqingyong/Works/Learn/Flink/Proj/github/Flink-Examples/src/main/resources/proj/UserBehavior.csv")

    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }

    producer.close()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
}
