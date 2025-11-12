package com.viettel.cdp.process;

import com.viettel.cdp.functions.RuleUtils;
import com.viettel.cdp.model.*;
import com.viettel.cdp.utils.BroadcastUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.log4j.Logger;

import java.time.Instant;
import java.util.List;

import static com.viettel.cdp.Main.*;

public class ValidateProcess extends BroadcastProcessFunction<UnifiedEvent, ControlMessage<?>, AlertEvent> {
    private static final Logger LOGGER = Logger.getLogger(ValidateProcess.class);

    @Override
    public void processElement(UnifiedEvent event, ReadOnlyContext ctx, Collector<AlertEvent> out) throws Exception {
        ReadOnlyBroadcastState<String, RuleSet> waState = ctx.getBroadcastState(RS_DESC);
        List<String> ruleSetIds = event.getRules();
        for (String rsId : ruleSetIds) {
            RuleSet ruleSet = waState.get(rsId);
            if (ruleSet == null) {
                LOGGER.warn("No RuleSet found for id: " + rsId);
                continue;
            }
            if ("INACTIVE".equals(ruleSet.getStatus())) {
                LOGGER.warn("RuleSet is INACTIVE for id: " + rsId);
                continue;
            }
            Boolean isValid = RuleUtils.evaluateFilter(ruleSet.getFilter(), event);
            if (isValid) {
                event.setValidateTime(Instant.now());
                AlertEvent alert = new AlertEvent(event.get_jsonpointer("/msisdn"), ruleSet.getSegmentID(), Instant.ofEpochMilli(event.getEventTime()), Instant.ofEpochMilli(event.getKafkaTimestamp()) , event.getFilterTime(), event.getAggTime(), event.getValidateTime(), Instant.now());
                out.collect(alert); // gửi ra DB/CEP
            }
        }

    }

    @Override
    public  void processBroadcastElement(ControlMessage<?> cm, Context ctx, Collector<AlertEvent> out) throws Exception {
        // Handle control messages if needed
        // Handle control messages if needed
        LOGGER.info("Received control message: " + cm.toString());
        switch (cm.getType()) {
            case WA: {
                BroadcastState<String, WindowAttribute> state = ctx.getBroadcastState(WA_DESC);
                BroadcastUtils.handleWindowAttribute((WindowAttribute) cm.getData(), cm.getAction(), state);
                break;
            }
            case RS: {
                BroadcastState<String, RuleSet> state = ctx.getBroadcastState(RS_DESC);
                BroadcastUtils.handleRuleSet((RuleSet) cm.getData(), cm.getAction(), state);
                break;
            }
            case INDEX: {
                BroadcastState<String, IndexEntity> state = ctx.getBroadcastState(INDEX_DESC);
                BroadcastUtils.handleIndexEntity((IndexEntity) cm.getData(), cm.getAction(), state);
                break;
            }
            default: {
                // TODO: log/DLQ nếu cần
                break;
            }
        }
    }
}
