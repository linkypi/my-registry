package org.hiraeth.govern.common.constant;

import org.hiraeth.govern.common.util.StringUtil;

/**
 * @author: leo
 * @description:
 * @ClassName: org.hiraeth.govern.common.constant
 * @date: 2023/11/27 12:32
 */
public enum NodeType {
    Master,
    Slave;

    public static NodeType of(String value){
        if(StringUtil.isEmpty(value)){
            return null;
        }

        for (NodeType item: NodeType.values()){
            if(item.name().toUpperCase().equals(value.toUpperCase())){
                return item;
            }
        }
        return null;
    }
}
