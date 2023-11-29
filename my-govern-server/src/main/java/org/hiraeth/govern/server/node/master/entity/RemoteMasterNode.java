package org.hiraeth.govern.server.node.master.entity;

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
public class RemoteMasterNode {
    private Integer nodeId;
    /**
     * 是否为controller候选节点
     */
    private boolean isControllerCandidate;

    public RemoteMasterNode(int nodeId, boolean isCandidate) {
        this.nodeId = nodeId;
        this.isControllerCandidate = isCandidate;
    }


    public ByteBuffer toBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(RequestType.NodeInfo.getValue());
        buffer.putInt(nodeId);
        buffer.putInt(isControllerCandidate ? 1 : 0);
        return buffer;
    }

    public static RemoteMasterNode parseFrom(ByteBuffer buffer) {
        int requestType = buffer.getInt();
        if (RequestType.NodeInfo.getValue() == requestType) {
            int nodeId = buffer.getInt();
            int isCandidate = buffer.getInt();
            return new RemoteMasterNode(nodeId, isCandidate == 1);
        }
        return null;
    }
}
