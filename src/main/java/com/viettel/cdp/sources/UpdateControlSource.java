package com.viettel.cdp.sources;

import com.viettel.cdp.model.ControlMessage;
import com.viettel.cdp.connector.KafkaSources;
import com.viettel.cdp.serde.MessageControlDeserialization;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.viettel.cdp.utils.Resource;

public class UpdateControlSource {

    private static KafkaSource<ControlMessage<?>> buildSource(boolean kerberosEnabled) {
        return KafkaSources.createKafkaSource(
            Resource.KAFKA_UPDATE_CONTROL.BOOTSTRAP_SERVERS,
            Resource.KAFKA_UPDATE_CONTROL.TOPIC_EVENTS,
            Resource.KAFKA_UPDATE_CONTROL.GROUP_ID,
            KafkaSources.getOffsetInitializer(Resource.KAFKA_UPDATE_CONTROL.AUTO_OFFSET_RESET),
            kerberosEnabled,
            Resource.KAFKA_UPDATE_CONTROL.SECURITY_PROTOCOL,
            Resource.KAFKA_UPDATE_CONTROL.SASL_MECHANISM,
            Resource.KAFKA_UPDATE_CONTROL.SASL_JAAS_CONFIG,
            Resource.KAFKA_UPDATE_CONTROL.JAAS_CONF_PATH,
            Resource.KAFKA_UPDATE_CONTROL.KRB5_CONF_PATH,
            new MessageControlDeserialization());
    }

public static DataStream<ControlMessage<?>> build(StreamExecutionEnvironment env, boolean kerberosEnabled) {
    KafkaSource<ControlMessage<?>> source = buildSource(kerberosEnabled);
    return KafkaSources.createEventStream(env, source, "message_control_unified", 1);
}

}
