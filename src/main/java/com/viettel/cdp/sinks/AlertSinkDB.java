package com.viettel.cdp.sinks;

import com.viettel.cdp.model.AlertEvent;
import com.viettel.cdp.utils.Resource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.flink.connector.jdbc.JdbcExecutionOptions;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;


public class AlertSinkDB {

    public static SinkFunction<AlertEvent> createAlertsJdbcSink() {
        return JdbcSink.<AlertEvent>sink(
                getInsertQuery(),
                getStatementSetter(),
                getJdbcExecutionOptions(),
                getJdbcConnectionOptions());
    }

    private static String getInsertQuery(){
        return "INSERT INTO alerts (msisdn, segment_id , event_time, kafka_timestamp, filter_time, agg_time, validate_time, push_time) VALUES (?, ?, ?, ?, ? ,? ,?, ?)";
    }

    private static JdbcExecutionOptions getJdbcExecutionOptions() {
        return JdbcExecutionOptions.builder()
                .withBatchSize(10000) // Set the batch size for the sink
                .withBatchIntervalMs(4000) // Set the batch interval in milliseconds
                .withMaxRetries(5) // Set the maximum number of retries
                .build();
    }

    private static JdbcStatementBuilder<AlertEvent> getStatementSetter(){
        return new JdbcStatementBuilder<AlertEvent>() {
            @Override
            public void accept(PreparedStatement ps, AlertEvent alert) throws SQLException {
                ps.setString(1, alert.getMsisdn());
                ps.setString(2, alert.getSegmentId());
                ps.setTimestamp(3, java.sql.Timestamp.valueOf(alert.getEventTime().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                ps.setTimestamp(4, java.sql.Timestamp.valueOf(alert.getKafkaTimestamp().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                ps.setTimestamp(5, java.sql.Timestamp.valueOf(alert.getFilterTime().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                ps.setTimestamp(6, java.sql.Timestamp.valueOf(alert.getAggTime().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                ps.setTimestamp(7, java.sql.Timestamp.valueOf(alert.getValidateTime().atZone(ZoneId.systemDefault()).toLocalDateTime()));
                ps.setTimestamp(8, java.sql.Timestamp.valueOf(alert.getPushTime().atZone(ZoneId.systemDefault()).toLocalDateTime()));

            }
        };
    }

    private static JdbcConnectionOptions getJdbcConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(Resource.ALERT_SINK.JDBC_DRIVER)
                .withUrl(Resource.ALERT_SINK.JDBC_URL)
                .withUsername(Resource.ALERT_SINK.JDBC_USERNAME)
                .withPassword(Resource.ALERT_SINK.JDBC_PASSWORD)
                .build();
    }
}
