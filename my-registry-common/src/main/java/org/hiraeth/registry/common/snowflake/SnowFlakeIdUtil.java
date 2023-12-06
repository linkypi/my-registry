package org.hiraeth.registry.common.snowflake;

/**
 * Description SnowFlakeIdUtil
 * Created by linxueqi on 2021/4/16 11:10
 */
public class SnowFlakeIdUtil {
    private final static SnowflakeIdWorker snowflakeIdWorker = new SnowflakeIdWorker(1,1);

    private SnowFlakeIdUtil() { throw new IllegalStateException("Utility class"); }

    public static long getNextId(){
        return snowflakeIdWorker.nextId();
    }
}
