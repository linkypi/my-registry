package org.hiraeth.govern.server.node.entity;

import cn.hutool.core.bean.BeanUtil;
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
public class Vote extends MessageBase{
    /**
     * 投票轮次
     */
    private Integer round;

    /**
     * 投票给哪个controller节点
     */
    private Integer targetNodeId;

    public Vote(int round, int targetNodeId){
        super();
        this.round = round;
        this.fromNodeId = targetNodeId;
        this.messageType = MessageType.Vote;
    }

    public Message toMessage(){
        ByteBuffer buffer = toBuffer();
        return new Message(messageType, buffer.array());
    }

    protected void writePayload(ByteBuffer buffer){
        buffer.putInt(round);
        buffer.putInt(targetNodeId);
    }

    public ByteBuffer toBuffer() {
        return super.newBuffer(8);
    }

    public static Vote parseFrom(MessageBase messageBase) {
        Vote vote = BeanUtil.copyProperties(messageBase, Vote.class);
        ByteBuffer buffer = messageBase.getBuffer();
        vote.round = buffer.getInt();
        vote.targetNodeId = buffer.getInt();
        return vote;
    }
}
