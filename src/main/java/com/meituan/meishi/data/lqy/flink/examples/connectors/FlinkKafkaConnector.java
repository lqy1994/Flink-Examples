package com.meituan.meishi.data.lqy.flink.examples.connectors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;

public class FlinkKafkaConnector {

    public static Kafka localConnect(String topic, String groupId) {
        return new Kafka()
                .version("universal")
                .topic(topic)
                .startFromEarliest()
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
                .property("group.id", "consume-kafka");
    }

    public static Json jsonDesc(TypeInformation schema, String jsonSchema) {
        return new Json()
                .failOnMissingField(true)
                .schema(schema)
                .jsonSchema(jsonSchema);
    }

}
