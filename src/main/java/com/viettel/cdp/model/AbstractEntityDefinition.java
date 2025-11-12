package com.viettel.cdp.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;


import java.time.Instant;
import java.util.List;

@lombok.Data
public abstract class AbstractEntityDefinition {
    @JsonProperty("id")
    protected String id;

    @JsonProperty("name")
    protected String name;

    @JsonProperty("version")
    protected String version;

    @JsonProperty("description")
    protected String description;

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class Sum {
        @JsonProperty("productions")
        private List<Production> productions;

        @JsonProperty("bias")
        private double bias;
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class AggExpression {
        @JsonProperty("sum")
        private Sum sum;
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class Production {
        @JsonProperty("fields")
        private List<Field> fields;

        @JsonProperty("w")
        private double w;
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class Field {
        @JsonProperty("fieldName")
        private String fieldName;

        @JsonProperty("inverse")
        private boolean inverse;

        @JsonProperty("source")
        private String source;

        @JsonProperty("tableName")
        private String tableName;

        @JsonProperty("sourceType")
        private String sourceType;

        @JsonProperty("isAgg")
        private boolean isAgg;
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class Filter {
        @JsonProperty("logical")
        private String logical;

        @JsonProperty("clauses")
        private List<Clause> clauses;

        @JsonProperty("children")
        private List<Filter> children;
    }

    @lombok.Data
    @lombok.Builder
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class Clause {
        @JsonProperty("sum")
        private Sum sum;

        @JsonProperty("operator")
        private String operator;

        @JsonProperty("dataType")
        private String dataType;

        @JsonProperty("value")
        private List<Object> value;
    }

    @JsonProperty("filter")
    private Filter filter;

    @JsonProperty("fieldDependencies")
    protected List<String> fieldDependencies;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    @JsonProperty("createdAt")
    protected Instant createdAt;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    @JsonProperty("updatedAt")
    protected Instant updatedAt;
}
