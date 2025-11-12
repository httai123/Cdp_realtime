package com.viettel.cdp.model;

import com.viettel.cdp.process.AggregateProcess;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

@lombok.Data
public class WindowState implements Serializable {
    private long expireTime;
    private final Bucket[] buffer;
    private final int bufferSize;
    private final long stepSizeMillis;

    private static final Logger LOGGER = Logger.getLogger(WindowState.class);

    public WindowState(long windowSizeMillis, long stepSizeMillis) {
        this.stepSizeMillis = stepSizeMillis;
        this.bufferSize = (int) (windowSizeMillis / stepSizeMillis);
        this.buffer = new Bucket[bufferSize];
        for (int i = 0; i < bufferSize; i++) {
            buffer[i] = new Bucket();
        }
    }

    public WindowState(long stepSizeMillis, long expireTime, int bufferSize) {
        this.stepSizeMillis = stepSizeMillis;
        this.expireTime = expireTime;
        this.bufferSize = bufferSize;
        this.buffer = new Bucket[bufferSize];
        for (int i = 0; i < bufferSize; i++) {
            buffer[i] = new Bucket();
        }
    }

    private int index(long timestamp) {
        // shift timestamp sang HCM (+7h)
        long hcmTs = timestamp + 7 * 60 * 60 * 1000L; // +7 giờ

        long slot = Math.floorDiv(hcmTs, stepSizeMillis);
        return (int) Math.floorMod(slot, bufferSize);
    }

    private long bucketStartTime(long timestamp) {
        long hcmTs = timestamp + 7 * 60 * 60 * 1000L; // +7 giờ
        long slot = Math.floorDiv(hcmTs, stepSizeMillis);

        // Trả về startMillis **theo UTC** vẫn hợp lệ để lưu ring buffer
        return slot * stepSizeMillis - 7 * 60 * 60 * 1000L;
    }

    private int index(long timestamp, ZoneId zoneId) {
        int offsetMillis = zoneId.getRules().getOffset(Instant.ofEpochMilli(timestamp)).getTotalSeconds() * 1000;
        long localTs = timestamp + offsetMillis;

        long slot = Math.floorDiv(localTs, stepSizeMillis);

        return (int) Math.floorMod(slot, bufferSize);
    }

    public Bucket getOrCreateBucket(long timestamp) {
        int idx = index(timestamp);
        long bucketStart = bucketStartTime(timestamp);
        Bucket b = buffer[idx];
        if (b.getStartTime() > bucketStart) {
            LOGGER.warn("Data too old, event time: " + timestamp + ", bucket start: " + bucketStart + ", current bucket start: " + b.getStartTime());
            return null; // bucket này còn mới hơn bucket cần tìm, tức là dữ liệu quá cũ rồi
        }
        if (b.getStartTime() < bucketStart) {
            b.reset(bucketStart);
        }
        return b;
    }


    public void addEvent(long timestamp, String attributeName, BigDecimal value, AggregationType aggregationType) {
        Bucket b = getOrCreateBucket(timestamp);
        if (b == null) {
            return; // dữ liệu quá cũ, không cần xử lý
        }
        b.updateAttribute(attributeName, value, aggregationType);

    }

    public Bucket getBucket(long timestamp) {
        int idx = index(timestamp);
        Bucket b = buffer[idx];
        long expectedStart = bucketStartTime(timestamp);
        return (b.getStartTime() == expectedStart) ? b : null;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    @lombok.NoArgsConstructor
    public class WindowAttributeValue implements  Serializable {
        private BigDecimal value;
        private Long count;

        public void update(BigDecimal input, AggregationType aggregationType) {
            switch (aggregationType) {
                case SUM:
                    if (value == null) value = BigDecimal.ZERO;
                    value = value.add(input);
                    break;
                case COUNT:
                    if (count == null) count = 0L;
                    count += 1;
                    break;
                case AVG:
                    // lưu sum vào value, count để chia sau
                    if (value == null) value = BigDecimal.ZERO;
                    if (count == null) count = 0L;
                    value = value.add(input);
                    count += 1;
                    break;
                case MAX:
                    if (value == null || input.compareTo(value) > 0) {
                        value = input;
                    }
                    break;
                case MIN:
                    if (value == null || input.compareTo(value) < 0) {
                        value = input;
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported aggregation type: " + aggregationType);
            }
        }

        public BigDecimal getResult(AggregationType type) {
            switch (type) {
                case SUM:
                    return value == null ? BigDecimal.ZERO : value;
                case COUNT:
                    return count == null ? BigDecimal.ZERO : BigDecimal.valueOf(count);
                case MAX:
                case MIN:
                    return value;
                case AVG:
                    if (value == null || count == null || count == 0) return BigDecimal.ZERO;
                    return value.divide(BigDecimal.valueOf(count), 10, RoundingMode.HALF_UP);
                default:
                    throw new IllegalArgumentException("Unsupported aggregation type: " + type);
            }
        }
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    @lombok.NoArgsConstructor
    public class Bucket implements Serializable {
        private long startTime;
        private Map<String, WindowAttributeValue> attributeValues;

        public void updateAttribute(String attributeName, BigDecimal value, AggregationType aggregationType) {
            if (value == null){
                return ;
            }
            WindowAttributeValue attributeValue = attributeValues.get(attributeName);
            if (attributeValue == null) {
                if (aggregationType == AggregationType.AVG) {
                    attributeValue = new WindowAttributeValue(BigDecimal.ZERO, 0L);
                } else if (aggregationType == AggregationType.COUNT) {
                    attributeValue = new WindowAttributeValue(null, 0L);
                }
                else if (aggregationType == AggregationType.MAX || aggregationType == AggregationType.MIN) {
                    attributeValue = new WindowAttributeValue(null, null);
                }
                else if (aggregationType == AggregationType.SUM) {
                    attributeValue = new WindowAttributeValue(BigDecimal.ZERO, null);
                }
                else {
                    throw new IllegalArgumentException("Unsupported aggregation type: " + aggregationType);
                }
                attributeValues.put(attributeName, attributeValue);
            }
            attributeValue.update(value, aggregationType);
        }

        public void reset(long newStartTime) {
            this.startTime = newStartTime;
            if (this.attributeValues == null) {
                this.attributeValues = new java.util.HashMap<>();
            } else {
                this.attributeValues.clear();
            }
        }
    }

    private long bucketStartTime(long timestamp, ZoneId zoneId) {
        int offsetMillis = zoneId.getRules().getOffset(Instant.ofEpochMilli(timestamp)).getTotalSeconds() * 1000;

        long localTs = timestamp + offsetMillis;
        long slot = Math.floorDiv(localTs, stepSizeMillis);

        // Trả về UTC millis
        return slot * stepSizeMillis - offsetMillis;
    }



    public static void main(String[] args) {
        long baseTs = 1758474000000L; // t giả định
        ZoneId zoneId = ZoneId.of("Asia/Ho_Chi_Minh");
        long index1 = new WindowState(172800000, 86400000).index(baseTs);
        long index2 = new WindowState(172800000, 86400000).index(baseTs + 1);
        long index3 = new WindowState(172800000, 86400000).index(baseTs, zoneId);
        long index4 = new WindowState(172800000, 86400000).index(baseTs + 1, zoneId);
        System.out.println(index1 + " - " + index2);
        System.out.println(index3 + " - " + index4);
    }
}