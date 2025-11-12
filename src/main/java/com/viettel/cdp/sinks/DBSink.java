package com.viettel.cdp.sinks;

import com.viettel.cdp.model.AlertEvent;
import com.viettel.cdp.model.UnifiedEvent;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.log4j.Logger;

public class DBSink implements SinkFunction<UnifiedEvent> {
    private static final Logger LOGGER = Logger.getLogger(DBSink.class);

    @Override
    public void invoke(UnifiedEvent value, Context context) {
        LOGGER.debug("Saving event to DB for msisdn: " + value.getPayload().get("msisdn"));
        // TODO: save to DB
    }
}