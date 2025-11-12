package com.viettel.cdp.functions;

import com.viettel.cdp.model.TimeUnit;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

public class TimeUtils {

    // Get the window bounds [windowStart, windowEnd) that contains the eventTime. eventTime in last slide of window
    public static long[] getWindowBounds(long eventTime,
                                         long windowSize,
                                         long slideStep,
                                         TimeUnit unit) {
        ZoneId zoneId = ZoneId.of("Asia/Ho_Chi_Minh");
        // convert epoch millis -> LocalDateTime
        Instant instant = Instant.ofEpochMilli(eventTime);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);

        // determine slide unit
        ChronoUnit chronoUnit = toChronoUnit(unit);

        // align to the NEXT step boundary (windowEnd)
        LocalDateTime windowEnd = alignToNextStep(dateTime, slideStep, unit);
        // windowStart = windowEnd - windowSize
        LocalDateTime windowStart = windowEnd.minus(windowSize, chronoUnit);

        // convert back to millis
        long startMillis = windowStart.atZone(zoneId).toInstant().toEpochMilli();
        long endMillis = windowEnd.atZone(zoneId).toInstant().toEpochMilli();

        return new long[]{startMillis, endMillis};
    }

    public static long getWindowStart(long eventTime, long windowSize, TimeUnit unit) {
        ZoneId zoneId = ZoneId.of("Asia/Ho_Chi_Minh");
        Instant instant = Instant.ofEpochMilli(eventTime);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);

        switch (unit) {
            case MONTH: {
                int month = dateTime.getMonthValue();
                int startMonth = ((month - 1) / (int) windowSize) * (int) windowSize + 1;
                int year = dateTime.getYear();
                return LocalDateTime.of(year, startMonth, 1, 0, 0)
                        .atZone(zoneId).toInstant().toEpochMilli();
            }
            case WEEK: {
                // Tuần tính từ Monday
                LocalDate monday = dateTime.toLocalDate().with(DayOfWeek.MONDAY);
                long weeksOffset = (long) ((dateTime.get(ChronoField.ALIGNED_WEEK_OF_YEAR) - 1) / (int) windowSize) * (int) windowSize;
                LocalDate startWeek = monday.minusWeeks(dateTime.get(ChronoField.ALIGNED_WEEK_OF_YEAR) - 1 - weeksOffset);
                return startWeek.atStartOfDay(zoneId).toInstant().toEpochMilli();
            }
            case DAY: {
                int day = dateTime.getDayOfMonth();
                int startDay = ((day - 1) / (int) windowSize) * (int) windowSize + 1;
                return LocalDateTime.of(dateTime.getYear(), dateTime.getMonth(), startDay, 0, 0)
                        .atZone(zoneId).toInstant().toEpochMilli();
            }
            case HOUR: {
                int hour = dateTime.getHour();
                int startHour = (hour / (int) windowSize) * (int) windowSize;
                return dateTime.withHour(startHour).withMinute(0).withSecond(0).withNano(0)
                        .atZone(zoneId).toInstant().toEpochMilli();
            }
            case MINUTE: {
                int minute = dateTime.getMinute();
                int startMinute = (minute / (int) windowSize) * (int) windowSize;
                return dateTime.withMinute(startMinute).withSecond(0).withNano(0)
                        .atZone(zoneId).toInstant().toEpochMilli();
            }
            case SECOND: {
                int second = dateTime.getSecond();
                int startSecond = (second / (int) windowSize) * (int) windowSize;
                return dateTime.withSecond(startSecond).withNano(0)
                        .atZone(zoneId).toInstant().toEpochMilli();
            }
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }


    // Get the last window end time that contains the eventTime
    public static long getLastWindowEnd(long eventTime,
                                        long windowSize,
                                        long slideStep,
                                        TimeUnit unit) {
        if (slideStep <= 0 || windowSize <= 0) {
            throw new IllegalArgumentException("slideStep and windowSize must be > 0");
        }

        ZoneId zoneId = ZoneId.of("Asia/Ho_Chi_Minh");
        Instant instant = Instant.ofEpochMilli(eventTime);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, zoneId);

        // first window end (the next step boundary)
        LocalDateTime firstEnd = alignToNextStep(dateTime, slideStep, unit);

        // number of windows that cover the event (ceiling division)
        long count = (windowSize + slideStep - 1) / slideStep; // = ceil(windowSize/slideStep)

        long additionalSteps = Math.max(0L, count - 1L);
        long deltaUnits = additionalSteps * slideStep;

        ChronoUnit chronoUnit = toChronoUnit(unit);
        LocalDateTime lastEnd = firstEnd.plus(deltaUnits, chronoUnit);

        return lastEnd.atZone(zoneId).toInstant().toEpochMilli();
    }

    private static LocalDateTime alignToNextStep(LocalDateTime dateTime, long step, TimeUnit unit) {
        switch (unit) {
            case MONTH: {
                int currentMonth = dateTime.getMonthValue();
                int nextMonthIndex = ((currentMonth - 1) / (int) step + 1) * (int) step + 1;
                int year = dateTime.getYear();
                if (nextMonthIndex > 12) {
                    nextMonthIndex = nextMonthIndex - 12;
                    year++;
                }
                return LocalDateTime.of(year, nextMonthIndex, 1, 0, 0);
            }
            case WEEK: {
                // tuần tính từ MONDAY -> align tới tuần tiếp theo
                LocalDate nextMonday = dateTime.toLocalDate().with(DayOfWeek.MONDAY).plusWeeks(step);
                return nextMonday.atStartOfDay();
            }
            case DAY: {
                int day = dateTime.getDayOfMonth();
                int nextDayIndex = ((day - 1) / (int) step + 1) * (int) step + 1;
                YearMonth ym = YearMonth.of(dateTime.getYear(), dateTime.getMonth());
                if (nextDayIndex > ym.lengthOfMonth()) {
                    // sang tháng sau
                    LocalDate firstDayNextMonth = ym.atDay(1).plusMonths(1);
                    return LocalDateTime.of(firstDayNextMonth.getYear(),
                            firstDayNextMonth.getMonth(),
                            ((nextDayIndex - 1) % ym.lengthOfMonth()) + 1, 0, 0);
                }
                return LocalDateTime.of(dateTime.getYear(), dateTime.getMonth(), nextDayIndex, 0, 0);
            }
            case HOUR: {
                int hour = dateTime.getHour();
                int nextHour = ((hour / (int) step) + 1) * (int) step;
                LocalDateTime base = dateTime.withMinute(0).withSecond(0).withNano(0);
                if (nextHour >= 24) {
                    return base.toLocalDate().plusDays(1).atStartOfDay();
                }
                return base.withHour(nextHour);
            }
            case MINUTE: {
                int minute = dateTime.getMinute();
                int nextMinute = ((minute / (int) step) + 1) * (int) step;
                LocalDateTime base = dateTime.withSecond(0).withNano(0);
                if (nextMinute >= 60) {
                    return base.plusHours(1).withMinute(0);
                }
                return base.withMinute(nextMinute);
            }
            case SECOND: {
                int second = dateTime.getSecond();
                int nextSecond = ((second / (int) step) + 1) * (int) step;
                LocalDateTime base = dateTime.withNano(0);
                if (nextSecond >= 60) {
                    return base.plusMinutes(1).withSecond(0);
                }
                return base.withSecond(nextSecond);
            }
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }

    public static long stepToMillis(long step, TimeUnit unit) {
        switch (unit) {
            case SECOND:
                return step * 1000L;
            case MINUTE:
                return step * 60_000L;
            case HOUR:
                return step * 3_600_000L;
            case DAY:
                return step * 86_400_000L;
            case WEEK:
                return step * 7 * 86_400_000L;
            case MONTH:
                // tháng không cố định (28-31 ngày)
                // => chọn trung bình 30 ngày hoặc throw exception
                throw new IllegalArgumentException("MONTH cannot be converted to fixed milliseconds");
            default:
                throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }


    private static ChronoUnit toChronoUnit(TimeUnit unit) {
        switch (unit) {
            case MONTH: return ChronoUnit.MONTHS;
            case WEEK: return ChronoUnit.WEEKS;
            case DAY: return ChronoUnit.DAYS;
            case HOUR: return ChronoUnit.HOURS;
            case MINUTE: return ChronoUnit.MINUTES;
            case SECOND: return ChronoUnit.SECONDS;
            default: throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
    }

    public static Instant parseToInstant(String dateTimeStr) {
        if (dateTimeStr == null || dateTimeStr.isEmpty()) {
            return null;
        }
        try {
            return Instant.parse(dateTimeStr); // expects ISO-8601 format: "2025-09-10T08:30:00Z"
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    // test
    public static void main(String[] args) {
        long now = 1758474000001L;
//        long[] win = getWindowBounds(now, 2, 1, TimeUnit.DAY);
//        ZoneId hcmZone = ZoneId.of("Asia/Ho_Chi_Minh");
//        System.out.println("EventTime: " + now);
//        System.out.println("Window: [" + win[0] + ", " + win[1] + ")");
//        System.out.println("EventTime  = " + Instant.ofEpochMilli(now).atZone(hcmZone));
//        System.out.println("WindowStart = " + Instant.ofEpochMilli(win[0]).atZone(hcmZone));
//        System.out.println("WindowEnd   = " + Instant.ofEpochMilli(win[1]).atZone(hcmZone));

        long start = getWindowStart(now, 1, TimeUnit.DAY);
        ZoneId hcmZone = ZoneId.of("Asia/Ho_Chi_Minh");
        System.out.println("EventTime   = " + Instant.ofEpochMilli(now).atZone(hcmZone));
        System.out.println("WindowStart = " + Instant.ofEpochMilli(start).atZone(hcmZone));
    }
}
