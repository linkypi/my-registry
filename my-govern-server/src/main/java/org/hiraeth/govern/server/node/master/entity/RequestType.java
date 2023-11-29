package org.hiraeth.govern.server.node.master.entity;

import lombok.Getter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 17:39
 */
@Getter
public enum RequestType {

    /**
     * 发送当前节点信息，包括 node.id, isControllerCandidate等信息
     */
    NodeInfo(1),
    /**
     * 发起投票
     */
    Vote(2);
    RequestType(int value){
        this.value = value;
    }
    private int value;
}
