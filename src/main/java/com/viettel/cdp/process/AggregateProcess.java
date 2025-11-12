package com.viettel.cdp.process;

import com.viettel.cdp.functions.ProcessingUtils;
import com.viettel.cdp.functions.TimeUtils;
import com.viettel.cdp.model.*;
import com.viettel.cdp.utils.BroadcastUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import static com.viettel.cdp.Main.*;

public class AggregateProcess extends KeyedBroadcastProcessFunction<String, UnifiedEvent, ControlMessage<?>, UnifiedEvent> {
    private static final Logger LOGGER = Logger.getLogger(AggregateProcess.class);
    private transient  MapState<StateKey, WindowState> windowState;
    private final MapStateDescriptor<StateKey, WindowState> windowStateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    TypeInformation.of(StateKey.class),
                    TypeInformation.of(WindowState.class));

    @Override
    public void open(Configuration parameters) {
        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    }

    @Override
    public void processElement(UnifiedEvent event, ReadOnlyContext ctx, Collector<UnifiedEvent> out) throws Exception {
        long eventTime = event.getEventTime();
        ReadOnlyBroadcastState<String, WindowAttribute> waState = ctx.getBroadcastState(WA_DESC);

        Map<StateKey, WindowState> states = new HashMap<>();
        Set<StateKey> updatedKeys = new HashSet<>();

        // Handle UPDATED and GET
        if (event.getUpdatedWas() != null && !event.getUpdatedWas().isEmpty()){
            handleWindowAttributes(eventTime, event.getUpdatedWas(), event, waState, states, true, updatedKeys, ctx);
        }
        if (event.getGetWas() != null && !event.getGetWas().isEmpty()) {
            handleWindowAttributes(eventTime, event.getGetWas(), event, waState, states, false, updatedKeys, ctx);
        }
        // Persist only updated states
        for (StateKey key : updatedKeys) {
            windowState.put(key, states.get(key));
        }

        LOGGER.debug("Aggregating event for msisdn: " +  event.get_jsonpointer("/msisdn"));
        event.setAggTime(Instant.now());
        out.collect(event);
    }

    private void handleWindowAttributes(
            long eventTime,
            List<String> waIds,
            UnifiedEvent event,
            ReadOnlyBroadcastState<String, WindowAttribute> waState,
            Map<StateKey, WindowState> states,
            boolean isUpdated,
            Set<StateKey> updatedKeys,
            ReadOnlyContext ctx
    ) throws Exception {
        for (String waId : waIds) {
            WindowAttribute wa = waState.get(waId);
            if (wa == null) {
                LOGGER.warn("No WindowAttribute found for ID: " + waId);
                continue;
            }
            if ("INACTIVE".equals(wa.getStatus())) {
                LOGGER.warn("WindowAttribute is INACTIVE for ID: " + waId + " with Name: " + wa.getName());
                continue;
            }

            WindowAttribute.TimeRange tr = wa.getTimeRange();

            StateKey stateKey = new StateKey(
                    event.getPayload().get("msisdn").toString(),
                    tr.getWindowSize(),
                    tr.getSlideStep(),
                    tr.getTimeUnit()
            );

            // Get state from cache or state backend
            WindowState state = states.computeIfAbsent(stateKey, key -> {
                try {
                    return windowState.get(key);
                } catch (Exception e) {
                    LOGGER.error("Error retrieving state for key: " + key, e);
                    return null;
                }
            });

            // If state is null and not in UPDATED mode, skip processing
            if (state == null && !isUpdated) {
                continue;
            }

            // Initialize state if null
            if (state == null) {
                long stepMillis = TimeUtils.stepToMillis(tr.getSlideStep(), tr.getTimeUnit());
                int bufferSize = tr.getWindowSize() / tr.getSlideStep();
                long eventExpireTime = TimeUtils.getLastWindowEnd(eventTime, tr.getWindowSize(), tr.getSlideStep(), tr.getTimeUnit());
                if (tr.getTtl() != null) {
                    eventExpireTime += tr.getTtl();
                }
                state = new WindowState(stepMillis, eventExpireTime, bufferSize);
                ctx.timerService().registerEventTimeTimer(eventExpireTime);
            }

            // Add event only in UPDATED mode
            if (isUpdated) {
                BigDecimal value = ProcessingUtils.getAggValueOfEvents(wa.getAggExpression().getSum(), event);
                if (value == null) {
                    LOGGER.warn("No value found for aggregation expression in event with key: " + event.get_jsonpointer("/msisdn"));
                }
                long eventExpireTime = TimeUtils.getLastWindowEnd(eventTime,
                        tr.getWindowSize(),
                        tr.getSlideStep(),
                        tr.getTimeUnit());
                if (tr.getTtl() != null) {
                    eventExpireTime += tr.getTtl();
                }

                state.addEvent(eventTime, wa.getId(), value, wa.getAggType());
                if (state.getExpireTime() < eventExpireTime) {
                    if (state.getExpireTime() > 0) {
                        ctx.timerService().deleteEventTimeTimer(state.getExpireTime());
                    }
                    state.setExpireTime(eventExpireTime);
                    ctx.timerService().registerEventTimeTimer(eventExpireTime);
                }
                updatedKeys.add(stateKey);
                states.put(stateKey, state);
            }

            BigDecimal value;
            if (Objects.equals(tr.getWindowType(), "SLIDING")){
                value = ProcessingUtils.getSlidingValue(wa, state, eventTime);
            }
            else if (Objects.equals(tr.getWindowType(), "TUMBLING")){
                value = ProcessingUtils.getTumblingValue(wa, state, eventTime);
            }
            else {
                LOGGER.warn("Unknown window type for WA: " + wa.getId() + " for event with key: " + event.get_jsonpointer("/msisdn"));
                continue;
            }
            if (value != null) {
                if (event.getWaValues() == null) {
                    event.setWaValues(new HashMap<>());
                }
                event.getWaValues().put(wa.getId(), value);
            } else {
                LOGGER.warn("Computed NaN value for WA: " + wa.getId() + " for event with key: " + event.get_jsonpointer("/msisdn"));
            }
        }
    }


    @Override
    public  void processBroadcastElement(ControlMessage<?> cm, Context ctx, Collector<UnifiedEvent> out) throws Exception {
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

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<UnifiedEvent> out) throws Exception {
        // duyệt qua windowState để clear những state có expireTime <= timestamp
        Iterator<Map.Entry<StateKey, WindowState>> it = windowState.entries().iterator();
        while (it.hasNext()) {
            Map.Entry<StateKey, WindowState> entry = it.next();
            if (entry.getValue().getExpireTime() <= timestamp) {
                it.remove(); // clear state
            }
        }
    }

}