package com.viettel.cdp.utils;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Properties;


public class KafkaUtils {
    public static <T> KafkaSink<T> createKafkaSink(
            String topic,
            Properties kafkaProps,
            SerializationSchema<T> serializationSchema
    ) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
                .setKafkaProducerConfig(kafkaProps)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<T>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(serializationSchema)
                                .build()
                )
                .build();
    }
}
