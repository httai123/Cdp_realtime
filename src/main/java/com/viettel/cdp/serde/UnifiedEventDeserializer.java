package com.viettel.cdp.serde;

import com.viettel.cdp.model.UnifiedEvent;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

public class UnifiedEventDeserializer implements KafkaRecordDeserializationSchema<UnifiedEvent> {
    private static final Logger LOGGER = Logger.getLogger(UnifiedEventDeserializer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<UnifiedEvent> out) {
        JsonNode node = null;
        try {
            if (record.value() != null) {

                node = MAPPER.readTree(record.value());
            }
        } catch (Exception ex) {
            // Log error và skip record này
            LOGGER.info("Failed to deserialize record: " + ex.getMessage());
            return;
        }

        Long eventTime = extractEventTime(node);
        String key = extractKey(node);

        UnifiedEvent event = new UnifiedEvent(
                record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                eventTime,
                node
        );
        out.collect(event);
    }

    private static Long extractEventTime(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }

        String[] candidates = {"event_time", "eventTime", "ts", "timestamp"};
        for (String field : candidates) {
            JsonNode v = n.get(field);
            if (v != null && !v.isNull()) {
                return parseTs(v);
            }
        }

        return null;
    }

    private static String extractKey(JsonNode n) {
        if (n == null) return null;
        JsonNode v;
        if ((v = n.get("msisdn")) != null) return v.asText();
        if ((v = n.get("MSISDN")) != null) return v.asText();
        if ((v = n.get("userId")) != null) return v.asText();
        if ((v = n.get("user_id")) != null) return v.asText();
        return null;
    }

    private static Long parseTs(JsonNode v) {
        try {
            if (v.isNumber()) return v.asLong();
            if (v.isTextual()) {
                String s = v.asText();
                if (s.matches("^-?\\d+$")) return Long.parseLong(s);
                return java.time.Instant.parse(s).toEpochMilli();
            }
        } catch (Exception ignore) {}
        return null;
    }

    @Override
    public TypeInformation<UnifiedEvent> getProducedType() {
        return TypeInformation.of(UnifiedEvent.class);

        // return org.apache.flink.api.java.typeutils.TypeExtractor.getForClass(UnifiedEvent.class);
    }
}
