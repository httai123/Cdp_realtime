package com.viettel.cdp.functions;

import com.viettel.cdp.model.AggregationType;
import com.viettel.cdp.model.UnifiedEvent;
import com.viettel.cdp.model.WindowAttribute;
import com.viettel.cdp.model.WindowState;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class ProcessingUtils {
    private static final Logger LOGGER = Logger.getLogger(ProcessingUtils.class);

    public static BigDecimal getSlidingValue(WindowAttribute windowAttribute, WindowState windowState, Long eventTime) {
        WindowAttribute.TimeRange tr = windowAttribute.getTimeRange();
        WindowState.Bucket[] buckets = windowState.getBuffer();
        long[] windowBounds = TimeUtils.getWindowBounds(
                eventTime,
                tr.getWindowSize(),
                tr.getSlideStep(),
                tr.getTimeUnit()
        );
        long startTime = windowBounds[0];
        long endTime = windowBounds[1];
        long maxTime = endTime;
        for (WindowState.Bucket bucket : buckets) {
            if (bucket.getStartTime() > maxTime) {
                maxTime = bucket.getStartTime();
            }
        }
        if (maxTime > endTime){
            startTime = maxTime - (endTime - startTime);
            endTime = maxTime;
        }

        return getWAValueInRange(windowAttribute, windowState, startTime, endTime);
    }

    public static BigDecimal getTumblingValue(WindowAttribute windowAttribute, WindowState windowState, Long eventTime) {
        WindowAttribute.TimeRange tr = windowAttribute.getTimeRange();
        long startTime = TimeUtils.getWindowStart(
                eventTime,
                tr.getSlideStep(),
                tr.getTimeUnit()
        );

        return getWAValueInTime(windowAttribute, windowState, startTime);
    }

    public static BigDecimal getWAValueInRange(WindowAttribute windowAttribute,
                                               WindowState windowState,
                                               Long startTime,
                                               Long endTime) {
        WindowState.Bucket[] buckets = windowState.getBuffer();
        AggregationType aggregationType = windowAttribute.getAggType();
        String waId = windowAttribute.getId();

        switch (aggregationType) {
            case SUM: {
                BigDecimal res = BigDecimal.ZERO;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() >= startTime && bucket.getStartTime() <= endTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null) {
                            res = res.add(waValue.getValue());
                        }
                    }
                }
                return res;
            }
            case COUNT: {
                long cnt = 0L;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() >= startTime && bucket.getStartTime() <= endTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getCount() != null) {
                            cnt += waValue.getCount();
                        }
                    }
                }
                return BigDecimal.valueOf(cnt);
            }
            case AVG: {
                BigDecimal sum = BigDecimal.ZERO;
                long cnt = 0L;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() >= startTime && bucket.getStartTime() <= endTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null && waValue.getCount() != null) {
                            sum = sum.add(waValue.getValue());
                            cnt += waValue.getCount();
                        }
                    }
                }
                if (cnt == 0) return BigDecimal.ZERO;
                return sum.divide(BigDecimal.valueOf(cnt), 5, RoundingMode.HALF_UP);
            }
            case MAX: {
                BigDecimal max = null;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() >= startTime && bucket.getStartTime() <= endTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null) {
                            if (max == null || waValue.getValue().compareTo(max) > 0) {
                                max = waValue.getValue();
                            }
                        }
                    }
                }
                return max == null ? BigDecimal.ZERO : max;
            }
            case MIN: {
                BigDecimal min = null;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() >= startTime && bucket.getStartTime() <= endTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null) {
                            if (min == null || waValue.getValue().compareTo(min) < 0) {
                                min = waValue.getValue();
                            }
                        }
                    }
                }
                return min == null ? BigDecimal.ZERO : min;
            }
            default:
                throw new IllegalArgumentException("Unsupported aggregation type: " + aggregationType);
        }
    }

    public static BigDecimal getWAValueInTime(WindowAttribute windowAttribute,
                                               WindowState windowState,
                                               Long startTime) {
        WindowState.Bucket[] buckets = windowState.getBuffer();
        AggregationType aggregationType = windowAttribute.getAggType();
        String waId = windowAttribute.getId();

        switch (aggregationType) {
            case SUM: {
                BigDecimal res = BigDecimal.ZERO;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() == startTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null) {
                            res = res.add(waValue.getValue());
                        }
                    }
                }
                return res;
            }
            case COUNT: {
                long cnt = 0L;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() == startTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getCount() != null) {
                            cnt += waValue.getCount();
                        }
                    }
                }
                return BigDecimal.valueOf(cnt);
            }
            case AVG: {
                BigDecimal sum = BigDecimal.ZERO;
                long cnt = 0L;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() == startTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null && waValue.getCount() != null) {
                            sum = sum.add(waValue.getValue());
                            cnt += waValue.getCount();
                        }
                    }
                }
                if (cnt == 0) return BigDecimal.ZERO;
                return sum.divide(BigDecimal.valueOf(cnt), 5, RoundingMode.HALF_UP);
            }
            case MAX: {
                BigDecimal max = null;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() == startTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null) {
                            if (max == null || waValue.getValue().compareTo(max) > 0) {
                                max = waValue.getValue();
                            }
                        }
                    }
                }
                return max == null ? BigDecimal.ZERO : max;
            }
            case MIN: {
                BigDecimal min = null;
                for (WindowState.Bucket bucket : buckets) {
                    if (bucket.getStartTime() == startTime) {
                        WindowState.WindowAttributeValue waValue = bucket.getAttributeValues().get(waId);
                        if (waValue != null && waValue.getValue() != null) {
                            if (min == null || waValue.getValue().compareTo(min) < 0) {
                                min = waValue.getValue();
                            }
                        }
                    }
                }
                return min == null ? BigDecimal.ZERO : min;
            }
            default:
                throw new IllegalArgumentException("Unsupported aggregation type: " + aggregationType);
        }
    }

    public static BigDecimal getAggValueOfEvents(WindowAttribute.Sum sum, UnifiedEvent event) {
        BigDecimal bias = BigDecimal.valueOf(sum.getBias());
        for (WindowAttribute.Production p : sum.getProductions()) {
            for (WindowAttribute.Field f : p.getFields()) {
//                if (!"REALTIME".equals(f.getSourceType())) {
//                    LOGGER.warn("Skipping non-REALTIME field: " + f.getFieldName());
//                    return null;
//                }
                BigDecimal val;
                if (f.isAgg()) {
                    Number raw = null;
                    if (event.getWaValues() != null){
                        raw = event.getWaValues().get(f.getFieldName());
                    }
                    if (raw != null) {
                        val = BigDecimal.valueOf(raw.doubleValue());
                    } else {
                        val = null;
                    }
                }
                else {
                    val = toBigDecimalOrNull(event.get_jsonpointer(f.getFieldName()));
                }
                if (val == null) {
                    LOGGER.warn("Field value is null or not a number for field: " + f.getFieldName());
                    return null;
                }
                if (f.isInverse()) {
                    val = val.negate();
                }

                bias = bias.add(val.multiply(BigDecimal.valueOf(p.getW())));
            }
        }
        return bias;
    }

    public static String getStringValueOfEvent(WindowAttribute.Sum sum, UnifiedEvent event) {
        if (sum.getProductions().size() != 1 || sum.getProductions().get(0).getFields().size() != 1) {
            LOGGER.warn("Sum should have exactly one production for string value extraction.");
            return null;
        }

        WindowAttribute.Field f = sum.getProductions().get(0).getFields().get(0);
        return event.get_jsonpointer(f.getFieldName());
    }


    public static BigDecimal toBigDecimalOrNull(String val) {
        if (val == null || val.trim().isEmpty()) {
            return null;
        }
        try {
            return new BigDecimal(val.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
