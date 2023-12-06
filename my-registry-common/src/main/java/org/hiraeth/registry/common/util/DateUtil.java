package org.hiraeth.registry.common.util;

import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * @desc
 * @auther linxueqi
 * @create 2020-04-16 13:37
 */
@Slf4j
public class DateUtil {

    public static final String EXCEL_FORMAT = "MM/dd/yyyy";
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd";
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String DATETIME_MINUTE_FORMAT = "yyyy-MM-dd HH:mm";
    public static final String YEAR_MONTH_FORMAT = "yyyy-MM";
    public static final String DATE_FORMAT = "yyyy年MM月dd日";
    public static final String YYYYMMDD = "yyyyMMdd";
    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    public static final String NOTICE_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String END_OF_DAY_FORMAT = "yyyy-MM-dd 23:59:59";
    public static final String HH_MM_SS = "HH:mm:ss";

    private DateUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static Date parseDate(String sDate, String format) throws ParseException {
        return new SimpleDateFormat(format).parse(sDate);
    }

    /**
     * 设置lenient为false,进行日期强校验，比如2018-02-29不会被接受
     *
     * @param sDate
     * @param dateFormat
     * @return
     * @throws ParseException
     */
    public static Date parseDateWithLenien(String sDate, String dateFormat) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        format.setLenient(false);
        return format.parse(sDate);
    }

    public static Date parse(String date) throws ParseException {
        return new SimpleDateFormat().parse(date);
    }

    /**
     *  解析日期类型
     * @param date
     * @param pattern  日期格式， 默认  yyyy-MM-dd
     * @return
     * @throws ParseException
     */
    public static Date parse(String date, String pattern) throws ParseException {
        if(StringUtil.isNullOrEmpty(pattern)){
            pattern = DEFAULT_FORMAT;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        return simpleDateFormat.parse(date);
    }

    public static Date parseExcelDate(String sDate) throws ParseException {
        return parseDate(sDate, EXCEL_FORMAT);
    }

    public static String format(Date date, String format){
        return new SimpleDateFormat(format).format(date);
    }

    /***
     * if format is not supplied , use {@link DateUtil#DATE_FORMAT}
     * @param date
     * @return date in String
     */
    public static String format(Date date){
        return new SimpleDateFormat(DEFAULT_FORMAT).format(date);
    }

    public static String format(String sDate,String pattern) throws ParseException {
        Date date = parse(sDate, DEFAULT_FORMAT);
        return format(date, pattern);
    }

    public static String format(String sDate, String parsePattern,String pattern) throws ParseException {
        Date date = parse(sDate, parsePattern);
        return format(date, pattern);
    }

    public static String addDayAndFormat(Date sDate, int num, String pattern){
        Calendar cal = Calendar.getInstance();
        cal.setTime(sDate);
        cal.add(Calendar.DATE, num);
        return format(cal.getTime(), pattern);
    }

    public static String addDayAndFormat(String sDate, int num, String pattern) throws ParseException {
        Calendar cal = Calendar.getInstance();
        cal.setTime(parse(sDate));
        cal.add(Calendar.DATE, num);
        return format(cal.getTime(), pattern);
    }

    public static String addDayAndFormat(String sDate, String parsePattern, int num, String pattern) throws ParseException {
        Calendar cal = Calendar.getInstance();
        cal.setTime(parse(sDate,parsePattern));
        cal.add(Calendar.DATE, num);
        return format(cal.getTime(), pattern);
    }

    public static Date addDayAndFormatToDate(Date sDate, int num, String pattern) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(sDate);
        cal.add(Calendar.DATE, num);
        return cal.getTime();
    }

    public static String addMonthAndFormat(String sDate, int num, String pattern) throws ParseException {
        Calendar cal = Calendar.getInstance();
        Date date = new SimpleDateFormat(DEFAULT_FORMAT).parse(sDate);
        cal.setTime(date);
        cal.add(Calendar.MONTH, num);
        return format(cal.getTime(), pattern);
    }

    public static String addMonthAndFormat(Date date, int num, String pattern) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, num);
        return format(cal.getTime(), pattern);
    }

    public static boolean isTheSameDay(String datetime1, String datetime2) throws ParseException {
        if (StringUtil.isNullOrEmpty(datetime1)) {
            return false;
        }
        if (StringUtil.isNullOrEmpty(datetime2)) {
            return false;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
        String date1 = simpleDateFormat.format(simpleDateFormat.parse(datetime1));
        return date1.equals(simpleDateFormat.format(simpleDateFormat.parse(datetime2)));
    }

    public static boolean isTheSameDay(Date datetime1, Date datetime2) {
        if (datetime1 == null) {
            return false;
        }
        if (datetime2 == null) {
            return false;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
        String date1 = simpleDateFormat.format(datetime1);
        return date1.equals(simpleDateFormat.format(datetime2));
    }

    /**
     * 指定日期增加天数
     *
     * @param sDate
     * @param num
     * @return
     */
    public static Date addDay(Date sDate, int num) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(sDate);
        cal.add(Calendar.DATE, num);
        return cal.getTime();
    }

    /**
     * 格式化Excel时间,读取excel时间为距离1900的天数
     *
     * @param day
     * @return yyyy-MM-dd
     */
    public static Date formatExcelDate(int day) {
        Calendar calendar = new GregorianCalendar(1900,0,-1);
        Date gregorianDate = calendar.getTime();
        return DateUtil.addDay(gregorianDate, day);
    }

    /**
     * 获取mysql存储时间上限
     *
     * @return
     */
    public static Date mysqlMaxDateTime() {
        try {
            return parse("9999-12-31 23:59:59", DATETIME_FORMAT);
        } catch (ParseException ex) {
            log.error("date parse error", ex);
        }
        return new Date();
    }

    /**
     * 获取excel最早存储时间限制
     *
     * @return
     */
    public static Date excelMinDateTime() {
        try {
            return parse("2000-01-01 00:00:00", DATETIME_FORMAT);
        } catch (ParseException ex) {
            log.error("date parse error", ex);
        }
        return new Date();
    }

    public static int getYear(Date date){
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return ca.get(Calendar.YEAR);
    }

    /**
     * 获取实际月份
     * @param date
     * @return
     */
    public static int getMonth(Date date){
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return ca.get(Calendar.MONTH) + 1;
    }

    /**
     * 获取实际月份
     * @param date
     * @return
     */
    public static int getDay(Date date){
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        return ca.get(Calendar.DAY_OF_MONTH) + 1;
    }

    /**
     * 获取某年第一天日期
     *
     * @param date
     * @return
     */
    public static Date getFirstDateOfYear(Date date){
        int year = getYear(date);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, year);
        return calendar.getTime();
    }

    /**
     * 获取某年最后一天日期
     * @param date 年份
     * @return Date
     */
    public static Date getLastDateOfYear(Date date){
        int year = getYear(date);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, year);
        calendar.roll(Calendar.DAY_OF_YEAR, -1);
        return calendar.getTime();
    }

    public static String getFirstFormatDateOfYear(Date date){
        Date first = getFirstDateOfYear(date);
        return format(first, DateUtil.DEFAULT_FORMAT);
    }

    /**
     * 获取某年最后一天日期 (yyyy-MM-dd)
     * @param date 年份
     * @return Date
     */
    public static String getLastFormatDateOfYear(Date date){
        Date last = getLastDateOfYear(date);
        return format(last, DateUtil.DEFAULT_FORMAT);
    }

    /**
     * 获取某年上一個月
     * @param date 年份
     * @return Date
     */
    public static String getLastMonth(Date date, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date); // 设置为当前时间
        calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) - 1); // 设置为上一个月
        date = calendar.getTime();
        String accDate = format.format(date);
        return accDate;
    }

    /**
     * 获取某个日期上个月第一天
     * @param date
     * @return
     */
    public static String getFirstDayOfLastMonthStr(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, -1);
        return format(calendar.getTime(), DEFAULT_FORMAT);
    }

    /**
     * 获取某个日期上个月最后一天
     * @param date
     * @return
     */
    public static String getLastDayOfLastMonthStr(Date date) {
        return format(getLastDayOfLastMonth(date), DEFAULT_FORMAT);
    }

    /**
     * 获取某个日期上个月最后一天
     * @param date
     * @return
     */
    public static Date getLastDayOfLastMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.DATE, -1);
        return calendar.getTime();
    }

    /**
     * 判断某个日期是否是给定日期的上个月的日期
     * @param verifyDate
     * @param nowDate
     * @return
     */
    public static boolean isLastMonthDate(Date verifyDate, Date nowDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(nowDate);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, -1);
        String nowDateStr = format(calendar.getTime(), DateUtil.YEAR_MONTH_FORMAT);
        String verifyDateStr = format(verifyDate, DateUtil.YEAR_MONTH_FORMAT);
        return nowDateStr.equals(verifyDateStr);
    }

    /***
     * Change {@link LocalDate} to {@link Date} by using {@link ZoneId#systemDefault()}
     * @param localDate
     * @return Date
     */
    public static Date fromLocaleDateSysTimeZone(LocalDate localDate){

        if (localDate == null) {
            return null;
        }

        //default time zone
        ZoneId defaultZoneId = ZoneId.systemDefault();
        //local date + atStartOfDay() + default time zone + toInstant() = Date
        Date date = Date.from(localDate.atStartOfDay(defaultZoneId).toInstant());
        return date;
    }

    /***
     * Change Date to LocalDate at {@link ZoneId#systemDefault()}
     * @param date
     * @return LocalDate
     */
    public static LocalDate fromDateSysTimeZone(Date date) {

        if (date == null) {
            return null;
        }

        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        return instant.atZone(zoneId).toLocalDate();
    }

    /**
     * 计算两个日期相差天数
     */
    public static long daysBetween(Date sDate, Date eDate) {
        LocalDate startTime = dateToLocalDate(sDate);
        LocalDate endTime = dateToLocalDate(eDate);
        return ChronoUnit.DAYS.between(startTime, endTime);
    }

    public static LocalDate dateToLocalDate(Date date) {
        if (null == date) {
            return null;
        }
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    // 获取本周的开始时间
    public static Date getBeginDayOfWeek(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int dayofweek = cal.get(Calendar.DAY_OF_WEEK);
        if (dayofweek == 1) {
            dayofweek += 7;
        }
        cal.add(Calendar.DATE, 2 - dayofweek);
        return getDayStartTime(cal.getTime());
    }

    // 获取某个日期的开始时间
    public static Timestamp getDayStartTime(Date d) {
        Calendar calendar = Calendar.getInstance();
        if (null != d)
            calendar.setTime(d);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
                calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return new Timestamp(calendar.getTimeInMillis());
    }

    /**
     * 获取当天的开始时间
     */
    public static Date atStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }
}
