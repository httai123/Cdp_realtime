package com.viettel.cdp.utils;

import com.viettel.cdp.model.*;
import com.viettel.cdp.process.AggregateProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.log4j.Logger;

import static com.viettel.cdp.Main.*;

public class BroadcastUtils {
    private static final Logger LOGGER = Logger.getLogger(BroadcastUtils.class);

    public static void handleWindowAttribute(
            WindowAttribute windowAttribute,
            String action,
            BroadcastState<String, WindowAttribute> state) throws Exception {
        upsertOrDelete(state, windowAttribute, action);
    }

    public static void handleRuleSet(
            RuleSet ruleSet,
            String action,
            BroadcastState<String, RuleSet> state) throws Exception {
        upsertOrDelete(state, ruleSet, action);
    }

    public static void handleIndexEntity(
            IndexEntity indexEntity,
            String action,
            BroadcastState<String, IndexEntity> state) throws Exception {
        upsertOrDelete(state, indexEntity, action);
    }

    private static <T> void upsertOrDelete(BroadcastState<String, T> state, T obj, String action) throws Exception {
        String key;
        if (obj instanceof IndexEntity) {
            key = "index_1";
        } else if (obj instanceof WindowAttribute) {
            key = ((WindowAttribute) obj).getId();
        } else if (obj instanceof RuleSet) {
            key = ((RuleSet) obj).getId();
        } else {
            key = null;
        }

        if (key == null) return;

        if ("DELETE".equalsIgnoreCase(action)) {
            state.remove(key);
        } else {
            state.put(key, obj);
        }
    }
}
