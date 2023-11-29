package org.hiraeth.govern.server.node.entity;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 18:40
 */
@Getter
@Setter
public class RemoteNode {
    private Integer nodeId;
    /**
     * 1: master
     * 2: slave
     */
    private NodeType nodeType;
    /**
     * 是否为controller候选节点
     */
    private boolean isControllerCandidate;

    public RemoteNode(int nodeId, NodeType nodeType, boolean isCandidate) {
        this.nodeId = nodeId;
        this.nodeType = nodeType;
        this.isControllerCandidate = isCandidate;
    }

    public ByteBuffer toBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(20);
        buffer.putInt(RequestType.NodeInfo.getValue());
        buffer.putInt(nodeId);
        buffer.putInt(nodeType == NodeType.Master ? 1 : 0);
        buffer.putInt(isControllerCandidate ? 1 : 0);
        return buffer;
    }

    public static RemoteNode parseFrom(ByteBuffer buffer) {
        int requestType = buffer.getInt();
        if (RequestType.NodeInfo.getValue() == requestType) {
            int nodeId = buffer.getInt();
            NodeType nodeType = buffer.getInt() == 1 ? NodeType.Master : NodeType.Slave;
            int isCandidate = buffer.getInt();
            return new RemoteNode(nodeId, nodeType, isCandidate == 1);
        }
        return null;
    }
}
