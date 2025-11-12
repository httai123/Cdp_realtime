// package com.viettel.cdp.serde;

// // UnifiedEventSerializationSchema.java
// import com.viettel.cdp.model.UnifiedEvent;
// import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
// import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
// import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
// import org.apache.kafka.clients.producer.ProducerRecord;
// import java.nio.charset.StandardCharsets;

// public class UnifiedEventSerializationSchema implements KafkaRecordSerializationSchema<UnifiedEvent> {
//     private static final ObjectMapper M = new ObjectMapper();
//     private final String targetTopic;

//     public UnifiedEventSerializationSchema(String targetTopic) { this.targetTopic = targetTopic; }

//     @Override
//     public ProducerRecord<byte[], byte[]> serialize(UnifiedEvent e, KafkaRecordSerializationSchema.KafkaSinkContext ctx, Long ts) {
//         try {
//             ObjectNode root = M.createObjectNode();
//             // metadata (giữ để truy vết)
//             root.put("source_topic", e.topic);
//             root.put("source_partition", e.partition);
//             root.put("source_offset", e.offset);
//             root.put("kafka_timestamp", e.kafkaTimestamp);
//             if (e.eventTime != null) root.put("event_time", e.eventTime);
//             if (e.headers != null) root.set("headers", M.valueToTree(e.headers));
//             // payload gốc
//             if (e.payload != null) root.set("payload", e.payload);

//             byte[] key = e.key == null ? null : e.key.getBytes(StandardCharsets.UTF_8);
//             byte[] value = M.writeValueAsBytes(root);
//             return new ProducerRecord<>(targetTopic, null, // partition: null → round-robin
//                     e.eventTime != null ? e.eventTime : ts, // timestamp của record
//                     key, value);
//         } catch (Exception ex) {
//             throw new RuntimeException(ex);
//         }
//     }
// }
