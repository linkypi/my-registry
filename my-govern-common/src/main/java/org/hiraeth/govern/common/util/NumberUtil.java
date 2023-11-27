package org.hiraeth.govern.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @desc
 * @auther linxueqi
 * @create 2020-04-16 13:37
 */
public class NumberUtil {
    /**
     * 转换Integer对象,失败返回NULL
     * @param obj
     * @return
     */
    public static Integer getInteger(Object obj) {
        return getInteger(obj, null);
    }

    public static Integer getInteger(Object obj, Integer defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return new Integer(obj.toString());
        } catch (NumberFormatException e) {
        }
        return defaultValue;
    }

    /**
     * 判断字符串是否为纯数字（整数）
     *
     * @param str
     * @return
     */
    public static boolean isNumeric(String str) {
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }

    /**
     * 判断字符串是否为数字（整数，小数，负数等）
     *
     * @param str
     * @return
     */
    public static boolean isDigital(String str) {
        Pattern pattern = Pattern.compile("-?[0-9]+(\\.[0-9]+)?");
        Matcher isNum = pattern.matcher(str);
        if (!isNum.matches()) {
            return false;
        }
        return true;
    }

    /**
     * 转换Long对象,失败返回NULL
     * @param obj
     * @return
     */
    public static Long getLong(Object obj) {
        return getLong(obj, null);
    }


    /**
     * 转换Long对象,失败返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static Long getLong(Object obj, Long defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return new Long(obj.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 转换Double对象,失败返回NULL
     * @param obj
     * @return
     */
    public static Double getDouble(Object obj) {
        return getDouble(obj, null);
    }


    /**
     * 转换Double对象,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static Double getDouble(Object obj, Double defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return new Double(obj.toString());
        } catch (NumberFormatException e) {
        }
        return defaultValue;
    }

    /**
     * 转换Float对象,失败返回NULL
     * @param obj
     * @return
     */
    public static Float getFloat(Object obj) {
        return getFloat(obj, null);
    }


    /**
     * 转换Float对象,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static Float getFloat(Object obj, Float defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return new Float(obj.toString());
        } catch (NumberFormatException e) {
        }
        return defaultValue;
    }

    /**
     * 转换Short对象,失败返回NULL
     * @param obj
     * @return
     */
    public static Short getShort(Object obj) {
        return getShort(obj, null);
    }


    /**
     * 转换Short对象,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static Short getShort(Object obj, Short defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return new Short(obj.toString());
        } catch (NumberFormatException e) {
        }
        return defaultValue;
    }

    /**
     * 转换Boolean对象,失败返回NULL
     * @param obj
     * @return
     */
    public static Boolean getBoolean(Object obj) {
        return getBoolean(obj, null);
    }


    /**
     * 转换Boolean对象,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static Boolean getBoolean(Object obj, Boolean defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        String s = obj.toString();
        if(s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return new Boolean(obj.toString());
        }
        return defaultValue;
    }

    /**
     * 转换为int基本类型,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static int convertInt(Object obj, int defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(obj.toString());
        } catch (Exception e) {
        }
        return defaultValue;
    }


    /**
     * 转换为long基本类型,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static long convertLong(Object obj, long defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(obj.toString());
        } catch (Exception e) {
        }
        return defaultValue;
    }


    /**
     * 转换为short基本类型,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static short convertShort(Object obj, short defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Short.parseShort(obj.toString());
        } catch (Exception e) {
        }
        return defaultValue;
    }


    /**
     * 转换为double基本类型,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static double convertDouble(Object obj, double defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(obj.toString());
        } catch (Exception e) {
        }
        return defaultValue;
    }


    /**
     * 转换为float基本类型,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static float convertFloat(Object obj, float defaultValue) {
        if(obj == null) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(obj.toString());
        } catch (Exception e) {
        }
        return defaultValue;
    }


    /**
     * 转换为boolean基本类型,转换失败则返回默认值
     * @param obj
     * @param defaultValue
     * @return
     */
    public static boolean convertBoolean(Object obj, boolean defaultValue) {
        if(obj == null || obj.toString().trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(obj.toString());
        } catch (Exception e) {
        }
        return defaultValue;

    }

}
