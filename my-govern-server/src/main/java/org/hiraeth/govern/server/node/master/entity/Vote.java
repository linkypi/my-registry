package org.hiraeth.govern.server.node.master.entity;

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
     * 投票节点id
     */
    private Integer voteNodeId;

    /**
     * 投票给哪个controller节点
     */
    private Integer voteForNodeId;

    /**
     * 投票轮次
     */
    private Integer voteRound;

    public ByteBuffer toBuffer(){
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putInt(RequestType.Vote.getValue());
        buffer.putInt(voteRound);
        buffer.putInt(voteNodeId);
        buffer.putInt(voteForNodeId);
        return buffer;
    }

    public static Vote parseFrom(ByteBuffer buffer){
        int requestType = buffer.getInt();
        if(RequestType.Vote.getValue() == requestType){
            int voteRound = buffer.getInt();
            int voteNodeId = buffer.getInt();
            int voteForNodeId = buffer.getInt();
            return new Vote(voteRound, voteNodeId, voteForNodeId);
        }
        return null;
    }
}
