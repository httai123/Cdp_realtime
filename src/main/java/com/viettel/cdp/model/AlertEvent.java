package com.viettel.cdp.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;

import java.time.Instant;

@lombok.Data
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class AlertEvent {
    private String msisdn;
    private String segmentId;
    private Instant eventTime; // nếu trích được từ payload
    private Instant kafkaTimestamp; // fallback
    private Instant filterTime;
    private Instant aggTime;
    private Instant validateTime;
    private Instant pushTime;
}
