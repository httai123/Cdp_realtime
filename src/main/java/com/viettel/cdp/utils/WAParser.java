package com.viettel.cdp.utils;

import com.viettel.cdp.model.WindowAttribute;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class WAParser {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WindowAttribute parseJson(String json) {
        try {
            return objectMapper.readValue(json, WindowAttribute.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON to WindowAttribute", e);
        }
    }
}
