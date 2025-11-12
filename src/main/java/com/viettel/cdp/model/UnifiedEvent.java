package com.viettel.cdp.model;

// UnifiedEvent.java

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

@lombok.Data
@lombok.Builder
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class UnifiedEvent {
    private String topic;
    private int partition;
    private long offset;
    private long kafkaTimestamp; // fallback
    private long eventTime; // nếu trích được từ payload
    private Instant filterTime;
    private Instant aggTime;
    private Instant validateTime;
    private JsonNode payload;
    private List<String> rules;
    private List<String> getWas;
    private List<String> updatedWas;
    private Map<String, Number> waValues;

    public UnifiedEvent(String topic, int partition, long offset, long kafkaTimestamp, long eventTime, JsonNode payload) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.kafkaTimestamp = kafkaTimestamp;
        this.eventTime = eventTime;
        this.payload = payload;
    }

    public String get_jsonpath(String path) {
        /*
         * Convert JsonNode to String and query path
         * input: String path
         * output: String value
         * 
         * example:
         * input: $.user.name
         * output: John Doe
         */
    
        if (payload == null || path == null || path.isEmpty())
            return null;
        try {
            Object v = JsonPath.read(payload.toString(), path); // dùng String để tránh xung đột shaded Jackson
            return v == null ? null : String.valueOf(v);
        } catch (PathNotFoundException e) {
            return null;
        }
    }

    public String get_jsonpointer(String path) {
        /*
         * Query value by JSON Pointer
         * input: String path (e.g. /user/name)
         * output: String value
         * 
         * example:
         * input: /user/name
         * output: John Doe
         */

        if (payload == null || path == null || path.isEmpty())
            return null;
        String pointer = path.startsWith("/") ? path : "/" + path;
        JsonNode node = payload.at(pointer);
        String val = node.isMissingNode() || node.isNull() ? null : node.asText();
        return val;
    }
}
