package com.viettel.cdp.functions;

import com.viettel.cdp.model.AbstractEntityDefinition;
import com.viettel.cdp.model.UnifiedEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class RuleUtils {
    public static Boolean evaluateFilter(AbstractEntityDefinition.Filter filter, UnifiedEvent event) {
        List<Boolean> childResults = new ArrayList<>();

        if (filter.getChildren() != null) {
            for (AbstractEntityDefinition.Filter child : filter.getChildren()) {
                childResults.add(evaluateFilter(child, event));
            }
        }

        if (filter.getClauses() != null) {
            for (AbstractEntityDefinition.Clause clause : filter.getClauses()) {
                childResults.add(evaluateClause(clause, event));
            }
        }

        String logical = filter.getLogical();
        if ("AND".equalsIgnoreCase(logical)) {
            return childResults.stream().allMatch(Boolean::booleanValue);
        } else if ("OR".equalsIgnoreCase(logical)) {
            return childResults.stream().anyMatch(Boolean::booleanValue);
        } else if ("NOT".equalsIgnoreCase(logical)) {
            // NOT should apply on a single child or clause
            if (childResults.size() != 1) {
                throw new IllegalArgumentException("NOT operator must have exactly one child or clause");
            }
            return !childResults.get(0);
        }
        return false;
    }

    public static  Boolean evaluateClause(AbstractEntityDefinition.Clause clause, UnifiedEvent event) {
        String dataType = clause.getDataType();
        switch (dataType) {
            case "String":
                return compareToString(clause, event);
            case "Long":
            case "Double":
            case "Integer":
            case "Float":
                return compareToNumber(clause, event);
            case "DateTime":
            case "Date":
                return compareToDate(clause, event);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

    }

    public static Boolean compareToNumber(AbstractEntityDefinition.Clause clause, UnifiedEvent event){
        BigDecimal val = ProcessingUtils.getAggValueOfEvents(clause.getSum(), event);
        if (val == null) {
            return false;
        }
        String operator = clause.getOperator();
        List<Object> values = clause.getValue();
        if (values == null || values.isEmpty()) {
            return false;
        }
        // Convert value[0] về BigDecimal an toàn
        Object raw = values.get(0);
        BigDecimal rhs;
        if (raw instanceof BigDecimal) {
            rhs = (BigDecimal) raw;
        } else if (raw instanceof Number) {
            rhs = BigDecimal.valueOf(((Number) raw).doubleValue());
        } else {
            try {
                rhs = new BigDecimal(raw.toString());
            } catch (Exception e) {
                throw new IllegalArgumentException("Value is not a valid number: " + raw, e);
            }
        }
        switch (operator) {
            case "==":
                return val.compareTo(rhs) == 0;
            case "!=":
                return val.compareTo(rhs) != 0;
            case ">":
                return val.compareTo(rhs) > 0;
            case "<":
                return val.compareTo(rhs) < 0;
            case ">=":
                return val.compareTo(rhs) >= 0;
            case "<=":
                return val.compareTo(rhs) <= 0;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    public static Boolean compareToString(AbstractEntityDefinition.Clause clause, UnifiedEvent event){
        String val = ProcessingUtils.getStringValueOfEvent(clause.getSum(), event);
        if (val == null) {
            return false;
        }
        String operator = clause.getOperator();
        List<Object> values = clause.getValue();
        if (values == null || values.isEmpty()) {
            return false;
        }

        switch (operator) {
            case "==":
                return val.equals(values.get(0).toString());
            case "!=":
                return !val.equals(values.get(0).toString());
            case "contains":
            case "like":
                return val.contains(values.get(0).toString());
            case "not like":
            case "not contains":
                return !val.contains(values.get(0).toString());
            case "in":
                return values.stream().anyMatch(v -> val.equals(v.toString()));
            case "not in":
                return values.stream().noneMatch(v -> val.equals(v.toString()));
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

    public static Boolean compareToDate(AbstractEntityDefinition.Clause clause, UnifiedEvent event){
        String val = ProcessingUtils.getStringValueOfEvent(clause.getSum(), event);
        Instant dateVal = TimeUtils.parseToInstant(val);
        if (dateVal == null) {
            return false;
        }

        String operator = clause.getOperator();
        List<Object> values = clause.getValue();
        if (values == null || values.isEmpty()) {
            return false;
        }
        switch (operator) {
            case "==":
                Instant eqDate = TimeUtils.parseToInstant(values.get(0).toString());
                return dateVal.equals(eqDate);
            case "!=":
                Instant neqDate = TimeUtils.parseToInstant(values.get(0).toString());
                return !dateVal.equals(neqDate);
            case ">":
                Instant gtDate = TimeUtils.parseToInstant(values.get(0).toString());
                return dateVal.isAfter(gtDate);
            case "<":
                Instant ltDate = TimeUtils.parseToInstant(values.get(0).toString());
                return dateVal.isBefore(ltDate);
            case ">=":
                Instant gteDate = TimeUtils.parseToInstant(values.get(0).toString());
                return dateVal.equals(gteDate) || dateVal.isAfter(gteDate);
            case "<=":
                Instant lteDate = TimeUtils.parseToInstant(values.get(0).toString());
                return dateVal.equals(lteDate) || dateVal.isBefore(lteDate);
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }

}
