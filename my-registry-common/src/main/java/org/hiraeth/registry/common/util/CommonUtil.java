package org.hiraeth.registry.common.util;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hiraeth.registry.common.constant.Constant.SLOTS_COUNT;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 12:38
 */
@Slf4j
public class CommonUtil {

    private static Pattern IP_REGEX_COMPILE = Pattern.compile("\\d+\\.\\d+\\.\\d+\\.\\d+");
    /**
     * for example:
     * 192.168.10.100:2156,192.168.10.110:2156
     */
    private static Pattern CLUSTER_REGEX_COMPILE = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)");

    public static int routeSlot(String key){
        if(StringUtil.isEmpty(key)){
            log.warn("route key cannot be empty.");
            return 0;
        }
        // 为防止负数需要 & 操作
        int hashCode = key.hashCode() & Integer.MAX_VALUE;
        return hashCode % SLOTS_COUNT;
    }

    public static String getString(Properties configProperties, String arg) {
        String value = configProperties.getProperty(arg);
        if (StringUtil.isEmpty(value)) {
            throw new IllegalArgumentException(arg + " cannot empty.");
        }
        log.debug("parameter {} = {}", arg, value);
        return value;
    }

    /**
     * 解析 127.0.0.1:9090,192.168.0.1:8989
     * @param configProperties
     * @param arg
     * @return
     */
    public static List<String> parseIpPortList(Properties configProperties, String arg) {
        String nodeServers = configProperties.getProperty(arg);
        if (StringUtil.isEmpty(nodeServers)) {
            throw new IllegalArgumentException(arg + " cannot be empty.");
        }
        String[] arr = nodeServers.split(",");
        if (arr.length == 0) {
            throw new IllegalArgumentException(arg + " cannot be empty.");
        }
        List<String> result = new ArrayList<>();
        for (String item : arr) {
            Matcher matcher = CLUSTER_REGEX_COMPILE.matcher(item);
            if (!matcher.matches()) {
                throw new IllegalArgumentException(arg + " parameters " + item + " is invalid.");
            }
            result.add(item);
        }
        return result;
    }

    public static String parseIP(Properties configProperties, String arg) {
        String nodeIp = configProperties.getProperty(arg);
        if (StringUtil.isEmpty(nodeIp)) {
            throw new IllegalArgumentException(arg + " cannot empty.");
        }
        Matcher matcher = IP_REGEX_COMPILE.matcher(nodeIp);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(arg + " parameters " + nodeIp + " is invalid.");
        }
        return nodeIp;
    }

    public static boolean parseBoolean(Properties configProperties, String arg, boolean defaultValue) {
        String boolStr = configProperties.getProperty(arg);
        if (StringUtil.isEmpty(boolStr)) {
            log.debug("parameter {} not specified, get default value: {}", arg, defaultValue);
            return defaultValue;
        }

        if ("true".equals(boolStr) || "false".equals(boolStr)) {
            log.debug("parameter {} = {}", arg, boolStr);
            return Boolean.parseBoolean(boolStr);
        }
        throw new IllegalArgumentException("controller.candidate must be true or false, not " + boolStr);
    }

    public static int parseInt(Properties configProperties, String arg){
        String value = "";
        int intValue = 0;
        try {
            value = configProperties.getProperty(arg);
            if (StringUtil.isEmpty(value)) {
                throw new IllegalArgumentException(arg + " cannot empty.");
            }

            intValue = Integer.parseInt(value);
        }catch (Exception ex){
            throw new IllegalArgumentException( "parameter " + arg +" is invalid "+ value);
        }
        log.debug("parameter {} = {}", arg, intValue);
        return intValue;
    }

    public static int parseInt(Properties configProperties, String arg, int defaultValue){
        String value = "";
        int intValue = 0;
        try {
            value = configProperties.getProperty(arg);
            if (StringUtil.isEmpty(value)) {
                return defaultValue;
            }

            intValue = Integer.parseInt(value);
        }catch (Exception ex){
            throw new IllegalArgumentException( "parameter " + arg +" is invalid "+ value);
        }
        log.debug("parameter {} = {}", arg, intValue);
        return intValue;
    }

    public static void writeStr(ByteBuffer buffer, String val){
        if(StringUtil.isEmpty(val)){
            buffer.putInt(0);
            return;
        }
        byte[] bytes = val.getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    public static void writeJsonString(ByteBuffer buffer, Object val){
        byte[] bytes = JSON.toJSONString(val).getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    public static int getStrLength(String val) {
        if (StringUtil.isEmpty(val)) {
            return 0;
        }
        byte[] bytes = val.getBytes();
        return bytes.length;
    }

    public static int getJsonStringLength(Object val) {
        byte[] bytes = JSON.toJSONString(val).getBytes();
        return bytes.length;
    }

    public static boolean readBoolean(ByteBuffer buffer) {
        int flag = buffer.getInt();
        return flag == 1;
    }
    public static String readStr(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length == 0) {
            return "";
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes);
    }

}
