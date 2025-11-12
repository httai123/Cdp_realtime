package com.viettel.cdp.sources;

import com.viettel.cdp.connector.KafkaSources;
import com.viettel.cdp.serde.UnifiedEventDeserializer;
import com.viettel.cdp.model.UnifiedEvent;
import com.viettel.cdp.utils.Resource;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EventSource {

    private static KafkaSource<UnifiedEvent> buildSource(boolean kerberosEnabled) {
            return KafkaSources.createKafkaSource(
                Resource.KAFKA_SOURCE.BOOTSTRAP_SERVERS,
                Resource.KAFKA_SOURCE.TOPIC_EVENTS,
                Resource.KAFKA_SOURCE.GROUP_ID,
                KafkaSources.getOffsetInitializer(Resource.KAFKA_SOURCE.AUTO_OFFSET_RESET),
                kerberosEnabled,
                Resource.KAFKA_SOURCE.SECURITY_PROTOCOL,
                Resource.KAFKA_SOURCE.SASL_MECHANISM,
                Resource.KAFKA_SOURCE.SASL_JAAS_CONFIG,
                Resource.KAFKA_SOURCE.JAAS_CONF_PATH,
                Resource.KAFKA_SOURCE.KRB5_CONF_PATH,
                new UnifiedEventDeserializer());
        
    }

    public static DataStream<UnifiedEvent> build(StreamExecutionEnvironment env, boolean kerberosEnabled) {
        KafkaSource<UnifiedEvent> source = buildSource(kerberosEnabled);
        return KafkaSources.createEventStream(env, source, "events_unified", 4);
    }
    
}
