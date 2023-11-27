package org.hiraeth.govern.common.util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;

/**
 * @desc
 * @auther linxueqi
 * @create 2023-11-23 11:01
 */
public class DatetimeUtil {

    public static final String DEFAULT_FORMAT = "yyyy-MM-dd";

    private DatetimeUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static Date formatTimeSlot(String timeSlot) {
        String[] ts = timeSlot.split(":");
        LocalDateTime td = LocalDate.now().atTime(Integer.valueOf(ts[0]), Integer.valueOf(ts[1]));
        return Date.from(td.atZone(ZoneId.systemDefault()).toInstant());
    }
}
