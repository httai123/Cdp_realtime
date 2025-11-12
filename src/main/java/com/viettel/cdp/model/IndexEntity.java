package com.viettel.cdp.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

@lombok.EqualsAndHashCode(callSuper = true)
@lombok.Data
@lombok.Builder
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class IndexEntity extends AbstractEntityDefinition {
    @JsonProperty("rev")
    private int rev;

    @JsonProperty("active_attrs")
    private List<String> activeAttrs;

    @JsonProperty("attrs_by_topic")
    private Map<String, List<String>> attrsByTopic;

    @JsonProperty("attrs_by_required_alias")
    private Map<String, List<String>> attrsByRequiredAlias;

    @JsonProperty("rules_by_attr")
    private Map<String, List<String>> rulesByAttr;

    @JsonProperty("attr_by_rules")
    private Map<String, List<String>> attrByRules;

    // --- new fields for v2 ---

    // attr -> required aliases to be present
    @JsonProperty("required_aliases_by_attr")
    private Map<String, List<String>> requiredAliasesByAttr;

    // alias -> df (document frequency) to pick pivots
    @JsonProperty("df_by_alias")
    private Map<String, Integer> dfByAlias;

    // buckets: pivot alias -> list of attr ids (or names)
    @JsonProperty("pivot_by_topic_b1")
    private Map<String, Map<String,List<String>>> pivotByTopicB1;

    @JsonProperty("pivot_by_topic_b2")
    private Map<String, Map<String,List<String>>> pivotByTopicB2;

    // optional signature for prefiltering (hex/base64 of 256-bit bloom)
    @JsonProperty("sig256")
    private Map<String, String> sig256;

    @Override
    public String toString() {
        return "IndexEntity{" +
                "rev=" + rev +
                ", activeAttrs=" + activeAttrs +
                ", attrsByTopic=" + attrsByTopic +
                ", attrsByRequiredAlias=" + attrsByRequiredAlias +
                ", rulesByAttr=" + rulesByAttr +
                ", requiredAliasesByAttr=" + requiredAliasesByAttr +
                ", dfByAlias=" + dfByAlias +
                ", pivotB1=" + pivotByTopicB1 +
                ", pivotB2=" + pivotByTopicB2 +
                ", sig256=" + sig256 +
                '}';
    }

    public Map<String, List<String>> getPivotB1(String topic) {
        return pivotByTopicB1.get(topic);
    }

    public Map<String, List<String>> getPivotB2(String topic) {
        return pivotByTopicB2.get(topic);
    }
    
}
