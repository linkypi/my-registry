package org.hiraeth.govern.server.node.master.entity;

import lombok.Getter;
import lombok.Setter;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 18:40
 */
@Getter
@Setter
public class RemoteMasterNode {
    private Integer nodeId;
    /**
     * 是否为controller候选节点
     */
    private boolean isControllerCandidate;
}
