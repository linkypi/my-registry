package org.hiraeth.govern.server.node.entity;

import lombok.Getter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 17:39
 */
@Getter
public enum MessageType {

    /**
     * 发送当前节点信息，包括 node.id, isControllerCandidate等信息
     */
    NodeInfo(1),
    /**
     * 发起投票
     */
    Vote(2),

    /**
     * leader已选举完成
     */
    ElectionComplete(3),
    ElectionCompleteAck(4),
    // 槽位分配
    AllocateSlots(5),
    AllocateSlotsAck(6)
    ;
    MessageType(int value){
        this.value = value;
    }
    private int value;

    public static MessageType of(int value){
        for(MessageType item: MessageType.values()){
            if(value == item.value){
                return item;
            }
        }
        return null;
    }
}
