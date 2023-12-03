package org.hiraeth.govern.server.entity;

import cn.hutool.core.bean.BeanUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.util.CommonUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 19:27
 */
@Getter
@Setter
@AllArgsConstructor
public class Vote extends ClusterBaseMessage{
    /**
     * 投票轮次
     */
    private Integer round;

    /**
     * 投票给哪个controller节点
     */
    private String targetNodeId;

    public Vote(int round, String targetNodeId){
        super();
        this.round = round;
        this.fromNodeId = targetNodeId;
        this.clusterMessageType = ClusterMessageType.Vote;
    }

    public ClusterMessage toMessage(){
        ByteBuffer buffer = toBuffer();
        return new ClusterMessage(clusterMessageType, buffer.array());
    }

    protected void writePayload(ByteBuffer buffer){
        buffer.putInt(round);
        writeStr(targetNodeId);
    }

    public ByteBuffer toBuffer() {
        int strLength = CommonUtil.getStrLength(targetNodeId);
        return convertToBuffer(8 + strLength);
    }

    public static Vote parseFrom(ClusterBaseMessage messageBase) {
        Vote vote = BeanUtil.copyProperties(messageBase, Vote.class);
        ByteBuffer buffer = messageBase.getBuffer();
        vote.round = buffer.getInt();
        vote.targetNodeId = messageBase.readStr();
        return vote;
    }
}
