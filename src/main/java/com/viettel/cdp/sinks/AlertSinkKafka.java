package com.viettel.cdp.sinks;

import com.viettel.cdp.functions.JsonSerializer;
import com.viettel.cdp.model.AlertEvent;
import com.viettel.cdp.utils.Resource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.util.Properties;

public class AlertSinkKafka {
    public static KafkaSink<String> createAlertsSink() throws IOException {
        Properties props = new Properties();
        if (Resource.ALERT_SINK.KERBEROS_ENABLED) {
            props.setProperty("security.protocol", Resource.ALERT_SINK.SECURITY_PROTOCOL);
            props.setProperty("sasl.mechanism", Resource.ALERT_SINK.SASL_MECHANISM);
            props.setProperty("sasl.kerberos.service.name", "kafka-dmp");
        }
        props.setProperty("bootstrap.servers", Resource.ALERT_SINK.BOOTSTRAP_SERVERS);
        String alertsTopic = Resource.ALERT_SINK.TOPIC_ALERTS;

        return KafkaSink.<String>builder()
                .setBootstrapServers(Resource.ALERT_SINK.BOOTSTRAP_SERVERS)
                .setKafkaProducerConfig(props) // inject full properties
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(alertsTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }

    public static DataStream<String> alertsStreamToJson(DataStream<AlertEvent> alerts) {
        return alerts.flatMap(new JsonSerializer<>(AlertEvent.class)).name("Alerts Deserialization");
    }
}