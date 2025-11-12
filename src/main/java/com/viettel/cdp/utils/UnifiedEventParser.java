package com.viettel.cdp.utils;

import com.viettel.cdp.model.UnifiedEvent;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class UnifiedEventParser {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static UnifiedEvent parse(String json) {
        try {
            return mapper.readValue(json, UnifiedEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse UnifiedEvent", e);
        }
    }
}
