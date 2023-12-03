package org.hiraeth.govern.common.domain;

import lombok.Getter;

/**
 * 客户端与服务器通信的消息类型
 * @author: lynch
 * @description:
 * @date: 2023/12/3 9:48
 */
@Getter
public enum MessageType {
    REQUEST(1),
    RESPONSE(2);

    MessageType(int val){
        this.value = val;
    }
    private int value;

    public static MessageType of(int value) {
        for (MessageType item : MessageType.values()) {
            if (item.value == value) {
                return item;
            }
        }
        return null;
    }
}
