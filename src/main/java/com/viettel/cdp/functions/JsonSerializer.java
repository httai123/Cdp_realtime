package com.viettel.cdp.functions;

import com.viettel.cdp.utils.JsonMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

public class JsonSerializer<T> extends RichFlatMapFunction<T, String> {
    private static final Logger LOGGER = Logger.getLogger(JsonSerializer.class);
    private JsonMapper<T> parser;
    private final Class<T> targetClass;

    public JsonSerializer(Class<T> sourceClass) {
        this.targetClass = sourceClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parser = new JsonMapper<>(targetClass);
    }

    @Override
    public void flatMap(T value, Collector<String> out) throws Exception {
        try {
            String serialized = parser.toString(value);
            out.collect(serialized);
        } catch (Exception e) {
            LOGGER.warn("Failed serializing to JSON dropping it:", e);
        }
    }
}