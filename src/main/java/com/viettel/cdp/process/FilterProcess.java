package com.viettel.cdp.process;

import com.viettel.cdp.functions.RuleUtils;
import com.viettel.cdp.model.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import com.viettel.cdp.model.IndexEntity;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;



public class FilterProcess extends BroadcastProcessFunction<UnifiedEvent, ControlMessage<?>, UnifiedEvent> {
    private static final Logger LOGGER = Logger.getLogger(FilterProcess.class);
    private final MapStateDescriptor<String, WindowAttribute> WA_DESC;
    private final MapStateDescriptor<String, RuleSet> RS_DESC;
    private final MapStateDescriptor<String, IndexEntity> INDEX_DESC;
    // cache theo rev để tránh rebuild mỗi event
    private transient int cachedRev = -1;
    private transient Map<String, List<String>> pivotB1;
    private transient Map<String, List<String>> pivotB2;
    private transient Map<String, List<String>> requiredAliasesByAttr;

    // constructor 3 tham số   do các tham số còn lại được cache
    public FilterProcess(MapStateDescriptor<String, WindowAttribute> WA_DESC, MapStateDescriptor<String, RuleSet> RS_DESC, MapStateDescriptor<String, IndexEntity> INDEX_DESC) {
        this.WA_DESC = WA_DESC;
        this.RS_DESC = RS_DESC;
        this.INDEX_DESC = INDEX_DESC;
    }

    @Override
    public void processBroadcastElement(ControlMessage<?> cm, Context ctx, Collector<UnifiedEvent> out) throws Exception {
        switch (cm.getType()) {
            case WA: {
                WindowAttribute wa = (WindowAttribute) cm.getData();
                BroadcastState<String, WindowAttribute> state = ctx.getBroadcastState(WA_DESC);
                upsertOrDelete(state, wa, cm.getAction());
                break;
            }
            case RS: {
                RuleSet rs = (RuleSet) cm.getData();
                BroadcastState<String, RuleSet> state = ctx.getBroadcastState(RS_DESC);
                upsertOrDelete(state, rs, cm.getAction());
                break;
            }
            case INDEX: {
                IndexEntity idx = (IndexEntity) cm.getData();
                BroadcastState<String, IndexEntity> state = ctx.getBroadcastState(INDEX_DESC);
                upsertOrDelete(state, idx, cm.getAction());
                break;
            }
            default: {
                // TODO: log/DLQ nếu cần
                break;
            }
        }
    }

    @Override
    public void processElement(UnifiedEvent ev, ReadOnlyContext ctx, Collector<UnifiedEvent> out) throws Exception {

        IndexEntity idx = ctx.getBroadcastState(INDEX_DESC).get("index_1");

        // ReadOnlyBroadcastState<String, IndexEntity> idxState = ctx.getBroadcastState(INDEX_DESC);
        if (idx == null) {
            LOGGER.error("Index is null");
            return;
        }

        if (cachedRev != idx.getRev()) {
            this.pivotB1 = idx.getPivotB1(ev.getTopic());
            this.pivotB2 = idx.getPivotB2(ev.getTopic());
            this.requiredAliasesByAttr = idx.getRequiredAliasesByAttr();
            this.cachedRev = idx.getRev();
        }
        if (pivotB1 == null && pivotB2 == null) return;
        if (requiredAliasesByAttr == null || requiredAliasesByAttr.isEmpty()) return;

        // 1) Tập field pivot cần kiểm tra xem event có các field này không
        Set<String> fieldUniverse = new HashSet<>();
        if (pivotB1 != null) fieldUniverse.addAll(pivotB1.keySet());
        if (pivotB2 != null) fieldUniverse.addAll(pivotB2.keySet());
        if (fieldUniverse.isEmpty()) return;
        
        // 2) Field có mặt trong event (probe trực tiếp theo JSON Pointer)
        Set<String> presentField = new HashSet<>();
        for (String field : fieldUniverse) {
            if (ev.get_jsonpointer(field) != null) presentField.add(field);
        }
        if (presentField.isEmpty()) return;

        // 3) Sinh ứng viên attr từ bucket pivot (chỉ field có mặt)
        Set<String> candidates = new HashSet<>();

        for (String field : presentField) { // duyệt qua các field có mặt trong event
            if (pivotB1 != null) {
                List<String> b1 = pivotB1.get(field);
                if (b1 != null) candidates.addAll(b1);
            }
            if (pivotB2 != null) {
                List<String> b2 = pivotB2.get(field);
                if (b2 != null) candidates.addAll(b2); // lấy các attr từ pivotB2
            }
        }

        if (candidates.isEmpty()) return;

        // 4) Verify chính xác: required_fields(attr) ⊆ presentField
        List<String> matchedAttrs = new ArrayList<>();
        for (String attr : candidates) {
            List<String> req = requiredAliasesByAttr.get(attr);
            if (req == null || req.isEmpty()) continue;
            boolean ok = true;
            for (String reqField : req) {
                if (!presentField.contains(reqField)) { ok = false; break; }
            }
            if (ok) matchedAttrs.add(attr);
        }  

        //4.1) Verify 100% bằng RuleUtils trên filter của WA
        List<String> toUpdateWas = new ArrayList<>();
        ReadOnlyBroadcastState<String, WindowAttribute> waState = ctx.getBroadcastState(WA_DESC);

        for (String waID : matchedAttrs) {
            if (waPassesFilter(waID, ev, waState)) {
                toUpdateWas.add(waID);
            }
        }

        // 5) lấy những attr liên quan
        
        Set<String> rules = new HashSet<>();
        Set<String> getWas = new HashSet<>();

        for (String attr : toUpdateWas) {
            List<String> ruleCandidates = idx.getRulesByAttr().get(attr);
            if (ruleCandidates != null) {
                rules.addAll(ruleCandidates);}
        }

        for (String rule : rules) {
            List<String> attrGets = idx.getAttrByRules().get(rule);
            if (attrGets == null) continue;
            for (String attrGet : attrGets) {
                if (!toUpdateWas.contains(attrGet)) {
                    getWas.add(attrGet);
                }
            }
        }


        if (!toUpdateWas.isEmpty()) {
            ev.setRules(new ArrayList<>(rules));
            ev.setGetWas(new ArrayList<>(getWas));
            ev.setUpdatedWas(toUpdateWas);
        }
        ev.setFilterTime(Instant.now());
        out.collect(ev);
    }

    private <T> void upsertOrDelete(BroadcastState<String, T> state, T obj, String action) throws Exception {
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

    private boolean waPassesFilter(
            String waID,
            UnifiedEvent ev,
            ReadOnlyBroadcastState<String, WindowAttribute> waState) throws Exception {

        WindowAttribute wa = waState.get(waID);
        if (wa == null) {
            LOGGER.warn("WA not found in broadcast state: " + waID);
            return false;
        }

        AbstractEntityDefinition.Filter filter = wa.getFilter();
        // Không có filter thì coi như pass
        if (filter == null) return true;

        try {
            Boolean ok = RuleUtils.evaluateFilter(filter, ev);
            return ok != null && ok;
        } catch (Exception e) {
            LOGGER.error("Error evaluating WA filter for waID=" + waID, e);
            return false;
        }
    }




}


