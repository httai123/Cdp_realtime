package com.viettel.cdp.serde;

import com.viettel.cdp.model.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.NullNode;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.log4j.Logger; 

public class MessageControlDeserialization implements KafkaRecordDeserializationSchema<ControlMessage<?>> {
    private static final Logger LOGGER = Logger.getLogger(MessageControlDeserialization.class);
    private static final ObjectMapper M = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> rec, Collector<ControlMessage<?>> out) {
        String json = rec.value() == null ? "" : new String(rec.value(), StandardCharsets.UTF_8);
        
        String typeHeader = Optional.ofNullable(rec.headers().lastHeader("entityType"))
                .map(h -> new String(h.value(), StandardCharsets.UTF_8)).orElse(null);
        String actionHeader = Optional.ofNullable(rec.headers().lastHeader("action"))
                .map(h -> new String(h.value(), StandardCharsets.UTF_8)).orElse(null);

        if (typeHeader == null) { // không có header → đẩy DLQ/UNKNOWN
            out.collect(new ControlMessage<>(EntityType.UNKNOWN, null, actionHeader, json));
            return;
        }
        try {
            // Nếu value có envelope {"payload":{...}} thì lấy payload; không thì dùng root
            JsonNode root = json.isEmpty() ? NullNode.getInstance() : M.readTree(json);
            JsonNode payload = root.has("payload") ? root.get("payload") : root;

            System.out.println("update control nè: " + payload);

            switch (typeHeader) {
                case "WA":
                    out.collect(ControlMessage.wa(
                            M.treeToValue(payload, WindowAttribute.class), actionHeader, json));
                    break;

                case "RS":
                    out.collect(ControlMessage.rs(
                            M.treeToValue(payload, RuleSet.class), actionHeader, json));
                    break;

                case "INDEX":
                    out.collect(ControlMessage.index(
                            M.treeToValue(payload, IndexEntity.class), actionHeader, json));
                    break;

                default:
                    out.collect(new ControlMessage<>(EntityType.UNKNOWN, null, actionHeader, json));
                    break;
            }
        } catch (Exception e) {
            // Handle deserialization errors (e.g., log the error or send to DLQ)
            LOGGER.error(String.format("Failed to deserialize control message. entityType=%s, action=%s, payload=%s",
                    typeHeader, actionHeader, json), e);
            // out.collect(new ControlMessage<>(EntityType.UNKNOWN, null, actionHeader, json));
        }
    }

    @Override
    public TypeInformation<ControlMessage<?>> getProducedType() {
        return TypeInformation.of(new TypeHint<ControlMessage<?>>() {
        });
    }
}
