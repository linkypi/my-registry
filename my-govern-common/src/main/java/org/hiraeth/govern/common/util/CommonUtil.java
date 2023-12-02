package org.hiraeth.govern.common.util;

import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Properties;

import static org.hiraeth.govern.common.constant.Constant.SLOTS_COUNT;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 12:38
 */
@Slf4j
public class CommonUtil {

    public static int routeSlot(String key){
        if(StringUtil.isEmpty(key)){
            log.warn("route key cannot be empty.");
            return 0;
        }
        // 为防止负数需要 & 操作
        int hashCode = key.hashCode() & Integer.MAX_VALUE;
        return hashCode % SLOTS_COUNT;
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

    public static int getStrLength(String val) {
        if (StringUtil.isEmpty(val)) {
            return 0;
        }
        byte[] bytes = val.getBytes();
        return bytes.length;
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
