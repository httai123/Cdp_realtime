package com.viettel.cdp.model;

import java.io.Serializable;

/**
 * ControlMessage bọc 1 bản tin control đã phân loại.
 * - type: WA | RS | INDEX | UNKNOWN
 * - data: object cụ thể (WindowAttribute | RuleSet | Index)
 * - action: CREATED | UPDATED | DELETED (nếu có trong envelope)
 * - rawJson: chuỗi JSON gốc (hữu ích khi debug/DLQ)
 */
@lombok.Data
@lombok.Builder
@lombok.NoArgsConstructor
@lombok.AllArgsConstructor
public class ControlMessage<T> implements Serializable {
    private EntityType type;
    private T data;
    private String action;
    private String rawJson;

    public static ControlMessage<WindowAttribute> wa(WindowAttribute data, String action, String raw) {
        return new ControlMessage<>(EntityType.WA, data, action, raw);
    }

    public static ControlMessage<RuleSet> rs(RuleSet data, String action, String raw) {
        return new ControlMessage<>(EntityType.RS, data, action, raw);
    }

    public static ControlMessage<IndexEntity> index(IndexEntity data, String action, String raw) {
        return new ControlMessage<>(EntityType.INDEX, data, action, raw);
    }

    // @Override
    // public String toString() {
    //     return "ControlMessage{type=" + type + ", action=" + action + ", data=" +
    //             (data == null ? "null" : data.getClass().getSimpleName()) + "}";
    // }
}
