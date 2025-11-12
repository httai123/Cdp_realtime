package com.viettel.cdp.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;


@lombok.EqualsAndHashCode(callSuper = true)
@lombok.Data
@lombok.Builder
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class RuleSet extends AbstractEntityDefinition {
    @JsonProperty("segmentID")
    private String segmentID;

    @JsonProperty("priority")
    private Integer priority;

    @JsonProperty("quantity")
    private Integer quantity;

    @JsonProperty("cooldownTime") // unit: minutes, default=0
    private Integer cooldownTime;

    @JsonProperty("status")
    private String status;

    @JsonProperty("windowAttributes")
    private List<String> windowAttributes;

    @Override
    public String toString() {
        return "RuleSet{" +
                "segmentID='" + segmentID + '\'' +
                ", priority=" + priority +
                ", quantity=" + quantity +
                ", cooldownTime=" + cooldownTime +
                ", filter=" + this.getFilter() +
                ", windowAttributes=" + windowAttributes +
                '}';
    }
}
