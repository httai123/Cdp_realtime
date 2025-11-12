package com.viettel.cdp.connector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

public final class KafkaSources {
	private KafkaSources() {}

	public static <T> KafkaSource<T> createKafkaSource(

			String bootstrapServers,
			String topic,
			String groupId,
			OffsetsInitializer offsets,
			boolean kerberosEnabled,
			String securityProtocol,
			String saslMechanism,
			String saslJaasConfig,
			String jaasConfPath,
			String krb5ConfPath,
			KafkaRecordDeserializationSchema<T> deserializer) {
		
		KafkaSourceBuilder<T> builder = KafkaSource.<T>builder()
				.setBootstrapServers(bootstrapServers)
				.setTopics(topic)
				.setGroupId(groupId)
				.setStartingOffsets(offsets)
				.setDeserializer(deserializer);

		if (kerberosEnabled) {
			builder.setProperty("security.protocol", securityProtocol);
			builder.setProperty("sasl.mechanism", saslMechanism);

			if (saslJaasConfig != null && !saslJaasConfig.isEmpty()) {
				builder.setProperty("sasl.jaas.config", saslJaasConfig);
			} else if (jaasConfPath != null && !jaasConfPath.isEmpty()) {
				System.setProperty("java.security.auth.login.config", jaasConfPath);
			}
			if (krb5ConfPath != null && !krb5ConfPath.isEmpty()) {
				System.setProperty("java.security.krb5.conf", krb5ConfPath);
			}
		}

		return builder.build();
	}


	public static <T> DataStream<T> createEventStream(
			StreamExecutionEnvironment env,
			KafkaSource<T> source,
			String name,
			int parallelism) {
		return env.fromSource(source, WatermarkStrategy.noWatermarks(), name).setParallelism(parallelism);
	}

	public static OffsetsInitializer getOffsetInitializer(String offsetReset) {
		return "earliest".equalsIgnoreCase(offsetReset)
				? OffsetsInitializer.earliest()
				: OffsetsInitializer.latest();
	}
}