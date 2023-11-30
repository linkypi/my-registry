package org.hiraeth.govern.server.node.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 19:27
 */
@Getter
@Setter
@AllArgsConstructor
public class Vote {
    /**
     * 投票轮次
     */
    private Integer round;
    /**
     * 投票节点id
     */
    private Integer fromNodeId;

    /**
     * 投票给哪个controller节点
     */
    private Integer targetNodeId;

    public ByteBuffer toBuffer(){
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(MessageType.Vote.getValue());
        buffer.putInt(round);
        buffer.putInt(fromNodeId);
        buffer.putInt(targetNodeId);
        return buffer;
    }

    public static Vote parseFrom(ByteBuffer buffer) {
        int voteRound = buffer.getInt();
        int voteNodeId = buffer.getInt();
        int voteForNodeId = buffer.getInt();
        return new Vote(voteRound, voteNodeId, voteForNodeId);
    }
}
