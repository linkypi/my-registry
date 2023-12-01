package org.hiraeth.govern.server.entity;

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
public class RemoteServer {
    private String nodeId;

    /**
     * 是否为controller候选节点
     */
    private boolean isControllerCandidate;

    public RemoteServer(String nodeId, boolean isCandidate) {
        this.nodeId = nodeId;
        this.isControllerCandidate = isCandidate;
    }

    public ByteBuffer toBuffer() {
        byte[] bytes = nodeId.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(20 + bytes.length);
        buffer.putInt(MessageType.NodeInfo.getValue());
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.putInt(isControllerCandidate ? 1 : 0);
        return buffer;
    }

    public static RemoteServer parseFrom(ByteBuffer buffer) {
        int requestType = buffer.getInt();
        if (MessageType.NodeInfo.getValue() == requestType) {
            int nodeIdLen = buffer.getInt();
            byte[] bytes = new byte[nodeIdLen];
            buffer.get(bytes);
            String nodeId = new String(bytes);
            int isCandidate = buffer.getInt();
            return new RemoteServer(nodeId, isCandidate == 1);
        }
        return null;
    }
}
