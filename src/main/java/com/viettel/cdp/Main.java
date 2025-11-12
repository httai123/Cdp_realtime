package com.viettel.cdp;

import com.viettel.cdp.model.*;
import com.viettel.cdp.process.AggregateProcess;
import com.viettel.cdp.process.ValidateProcess;
import com.viettel.cdp.sinks.AlertSinkDB;
import com.viettel.cdp.utils.Resource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.viettel.cdp.process.FilterProcess;
import com.viettel.cdp.sources.EventSource;
import com.viettel.cdp.sources.UpdateControlSource;
import org.apache.flink.streaming.api.datastream.BroadcastStream;

import java.time.Duration;

public class Main
{
    // ====== Broadcast State Descriptors (3 map state) ======
    public static final MapStateDescriptor<String, WindowAttribute> WA_DESC =
            new MapStateDescriptor<>("wa-state", String.class, WindowAttribute.class);
    public static final MapStateDescriptor<String, RuleSet> RS_DESC =
            new MapStateDescriptor<>("rs-state", String.class, RuleSet.class);
    public static final MapStateDescriptor<String, IndexEntity> INDEX_DESC =
            new MapStateDescriptor<>("index-state", String.class, IndexEntity.class);


    public static void main(String[] args) throws Exception {

        // 1. Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (Resource.ROCKSDB_CONFIG.ENABLE_CHECKPOINT) {
            long checkpointInterval = Resource.ROCKSDB_CONFIG.CHECKPOINTS_INTERVAL;
            long minPauseBetweenCheckpoints = Resource.ROCKSDB_CONFIG.MIN_PAUSE_BETWEEN_CHECKPOINTS;
            env.enableCheckpointing(checkpointInterval);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
            EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(true);
            backend.setNumberOfTransferThreads(8);
            backend.setWriteBatchSize(8);
            env.setStateBackend(backend);
            env.getCheckpointConfig().setCheckpointStorage("file:///u01/flink-1.20.1/flink-checkpoints");
            env.setParallelism(Resource.ROCKSDB_CONFIG.NUMBER_PARALLELISM);
            env.getCheckpointConfig().enableUnalignedCheckpoints(Resource.ROCKSDB_CONFIG.ENABLE_CHECKPOINT_UNALIGNED);
            env.getConfig().setAutoWatermarkInterval(3000);
        }
        
        // KafkaSource<UnifiedEvent> source = KafkaSources.createKafkaSource(new UnifiedEventDeserializer());
        // DataStream<UnifiedEvent> events = KafkaSources.createEventStream(env, source, "events_unified");

        // 2. Data Source
        DataStream<UnifiedEvent>  events = EventSource.build(env, Resource.KAFKA_SOURCE.KERBEROS_ENABLED).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<UnifiedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner((event, ts) -> event.getEventTime())
        );

        DataStream<ControlMessage<?>> controlMessages = UpdateControlSource.build(env, Resource.KAFKA_UPDATE_CONTROL.KERBEROS_ENABLED).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<ControlMessage<?>>forMonotonousTimestamps()
                        .withTimestampAssigner((msg, ts) -> Long.MAX_VALUE)
        );
        BroadcastStream<ControlMessage<?>> controlBc = controlMessages.broadcast(WA_DESC, RS_DESC, INDEX_DESC);


        // 3. Filter
       DataStream<UnifiedEvent> filtered = events
               .connect(controlBc)
               .process(new FilterProcess(WA_DESC, RS_DESC, INDEX_DESC))
               .name("FilterProcess")
               .setParallelism(4);

        // 4. Aggregate
        DataStream<UnifiedEvent> aggregated = filtered
                .keyBy((UnifiedEvent event) -> {
                    // giả sử payload của UnifiedEvent là JsonNode
                    JsonNode payload = event.getPayload();
                    if (payload != null && payload.has("msisdn")) {
                        return payload.get("msisdn").asText();
                    }
                    return "UNKNOWN"; // fallback nếu không có msisdn
                })
                .connect(controlBc)
                .process(new AggregateProcess())
                .name("AggregateProcess");

        // 5. Validate
        DataStream<AlertEvent> validated = aggregated
                .connect(controlBc)
                .process(new ValidateProcess())
                .name("ValidateProcess");

        // 6. Sink
        validated.addSink(AlertSinkDB.createAlertsJdbcSink());
//        validated.addSink(new CEPOutputSink());
        filtered.print().name("debug-print");
        env.execute("Flink test Job");
        System.out.println("Hello World3");
    }
}
