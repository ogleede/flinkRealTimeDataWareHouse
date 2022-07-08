package com.ogleede.gmalllogger.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author Ogleede
 * @Description
 * LocalDate 年月日
 * LocalTime 时分秒
 * LocalDateTime 年月日时分秒
 *
 * 以上三个工具类，不同于sdf，是线程安全的
 * @create 2022-06-29-21:34
 */
public class DateTimeUtil {
    private final static DateTimeFormatter formater =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(
                date.toInstant(),
                ZoneId.systemDefault());
        return formater.format(localDateTime);
    }

    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formater);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
