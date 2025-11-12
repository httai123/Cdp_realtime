package com.viettel.cdp.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@lombok.EqualsAndHashCode(callSuper = true)
@lombok.Data
@lombok.Builder
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class WindowAttribute extends AbstractEntityDefinition {

    @JsonProperty("status")
    private String status;

    @JsonProperty("aggType")
    private AggregationType aggType;

    @JsonProperty("groupingKeyNames")
    private List<String> groupingKeyNames;

    @JsonProperty("sourceID")
    private String sourceID;

    @JsonProperty("aggExpression")
    private AggExpression aggExpression;

    @JsonProperty("timeRange")
    private TimeRange timeRange;



    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class TimeRange {
        @JsonProperty("field")
        private String field;

        @JsonProperty("windowType")
        private String windowType;

        @JsonProperty("windowSize")
        private Integer windowSize;

        @JsonProperty("slideStep")
        private Integer slideStep;

        @JsonProperty("sessionTimeout")
        private Integer sessionTimeout;

        @JsonProperty("timeUnit")
        private TimeUnit timeUnit;

        @JsonProperty("ttl")
        private Integer ttl;

        @JsonProperty("timeZone")
        private String timeZone;

        @JsonProperty("includeBoundary")
        private String includeBoundary;
    }

    @Override
    public String toString() {
        return "WindowAttribute{" +
                "status='" + status + '\'' +
                ", aggType='" + aggType + '\'' +
                ", groupingKeyNames=" + groupingKeyNames +
                ", sourceID='" + sourceID + '\'' +
                ", aggExpression=" + aggExpression +
                ", filter=" + this.getFilter() +
                ", timeRange=" + timeRange +
                '}';
    }
}
