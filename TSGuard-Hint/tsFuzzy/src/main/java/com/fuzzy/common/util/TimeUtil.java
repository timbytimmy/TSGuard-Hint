package com.fuzzy.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimeUtil {

    /**
     * 将标准时间转为Unix秒
     *
     * @param date String格式时间
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long dateStringToUTCSeconds(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.parse(date).getTime() / 1000;
    }

    public static long dayDateStringToUTCSeconds(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.parse(date).getTime() / 1000;
    }

    /**
     * 将标准时间转为Unix毫秒
     *
     * @param date String格式时间
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long dateStringToUTCMilliSeconds(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.parse(date).getTime();
    }

    public static long llnwDateStringToUTCMilliSeconds(String date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        return format.parse(date).getTime();
    }

    public static Date xSfdDateStringToDate(String xSfdDate) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        return format.parse(xSfdDate);
    }

    public static long cfLogFileDateStringToTime(String cfLogFileDateString) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        return format.parse(cfLogFileDateString).getTime();
    }

    public static String dateToxSfdDateString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        try {
            return format.format(date);
        } catch (Exception e) {
            return null;
        }
    }

    public static String dateToUTCString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return format.format(date);
        } catch (Exception e) {
            return "";
        }
    }

    public static String dateToUTCDayString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        try {
            return format.format(date);
        } catch (Exception e) {
            return "";
        }
    }

    public static String dateToUTCMonthString(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
        try {
            return format.format(date);
        } catch (Exception e) {
            return "";
        }
    }

    public static String dateToISO8601String(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy/M/d");
        try {
            return format.format(date);
        } catch (Exception e) {
            return "";
        }
    }

    public static String dateToCertName(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            return format.format(date);
        } catch (Exception e) {
            return "";
        }
    }

    public static String utcMillisToISO8601String(long utcMillis) {
        // 将 UTC 时间戳（毫秒）转换为 Instant 对象
        Instant instant = Instant.ofEpochMilli(utcMillis);

        // 将 Instant 对象转换为指定时区的 ZonedDateTime 对象（这里是 UTC+8）
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("UTC"));

        // 定义 ISO 8601 格式的 DateTimeFormatter
        DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

        // 格式化 ZonedDateTime 对象为 ISO 8601 字符串
        return zonedDateTime.format(formatter);
    }


    public static boolean isValidDateString(String date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            format.parse(date);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static Date UTCMilliSecondsToNullableDate(long date) {
        return dateStringToNullableDate(uTCMilliSecondsToDateString(date));
    }

    public static String dateTimeStringToDate(String time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date dateTime = dateStringToNullableDate(time);
        if (dateTime == null) return time;
        long timeMills = dateTime.getTime();
        timeMills = timeMills - 1000 * 3600 * 8;
        return sdf.format(timeMills);
    }

    public static String timestampToISO8601(Long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Date dateTime = new Date(timestamp);
        return sdf.format(dateTime);
    }

    public static String timestampToRFC3339(Long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSX");
        Date dateTime = new Date(transTimestampToMS(timestamp));
        return sdf.format(dateTime);
    }

    public static Long transTimestampToMS(Long timestamp) {
        Long res = timestamp;
        if (String.valueOf(timestamp).length() == 10) {
            res *= 1000;
        } else if (String.valueOf(timestamp).length() == 13) {

        } else if (String.valueOf(timestamp).length() == 16) {
            res /= 1000;
        } else if (String.valueOf(timestamp).length() == 19) {
            res /= 1000 * 1000;
        } else {
            throw new AssertionError();
        }
        return res;
    }

    public static Date dateStringToNullableDate(String date) {
        if (date == null || "".equals(date)) return null;
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return format.parse(date);
        } catch (Exception e) {
            return null;
        }
    }

    public static Date monthStringToNullableDate(String date) {
        if (date == null || "".equals(date)) return null;
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
            return format.parse(date);
        } catch (Exception e) {
            return null;
        }
    }

    public static String dateToUTCStringLog(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        try {
            return sdf.format(date);
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcSeconds long格式当前unix时间秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCSecondsToDateString(long utcSeconds) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date(utcSeconds * 1000));
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcMilliSeconds long格式当前unix时间毫秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCMilliSecondsToDateString(long utcMilliSeconds) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date(utcMilliSeconds));
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcMilliSeconds long格式当前unix时间毫秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCMilliSecondsToNullableDateString(Long utcMilliSeconds) {
        if (utcMilliSeconds == null) {
            return null;
        }
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(new Date(utcMilliSeconds));
    }

    public static Date uTCMilliSecondsToDate(long utcMilliSeconds) {
        return new Date(utcMilliSeconds);
    }

    /**
     * 将Unix秒转为标准时间
     *
     * @param utcMilliSeconds long格式当前unix时间毫秒数
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String uTCMilliSecondsToDateStringWithMs(long utcMilliSeconds) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        return format.format(new Date(utcMilliSeconds));
    }

    public static String currentDateStringWithMs() {
        long utcMilliSeconds = System.currentTimeMillis();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        return format.format(new Date(utcMilliSeconds));
    }

    public static String dateStringForFileName(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss");
        return format.format(date);
    }

    public static String uTCMSToDateString(String utcSeconds) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long second = Long.parseLong(utcSeconds.substring(0, 10));
        return (format.format(new Date(second * 1000))) + ":" + utcSeconds.substring(10);
    }

    /**
     * 当前时间的年份
     *
     * @param date 当前时间
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String dateStringYear(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy");
        return format.format(date);
    }

    /**
     * 当前时间的月份
     *
     * @param date 当前时间
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String dateStringMonth(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("MM");
        return format.format(date);
    }

    /**
     * 当前时间的年月份
     *
     * @param date 当前时间
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String dateStringYearMonth(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
        return format.format(date);
    }

    /**
     * 当前时间的年月份周
     *
     * @param date 当前时间
     * @return String
     * @author none
     * @date 2020-11-05 00:00
     */
    public static String dateStringYearMonthDay(Date date) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        return format.format(date);
    }

    /**
     * 返回当前时间的Unix值，单位为毫秒
     *
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long currentUTCSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * 返回当前时间的Unix值，单位为毫秒
     *
     * @return long
     * @author none
     * @date 2020-11-05 00:00
     */
    public static long currentUTCMilliSeconds() {
        return System.currentTimeMillis();
    }
}
