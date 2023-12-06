package org.hiraeth.registry.common.util;

import java.util.List;

/**
 * @author linxueqi
 * @description
 * @createtime 2020-07-13 14:12
 */
public class CollectionUtil {
    public static <T> boolean isNullOrEmpty(List<T> list){
        return null == list || list.size() == 0;
    }

    public static <T> boolean notEmpty(List<T> list){
        return !isNullOrEmpty(list);
    }
}
