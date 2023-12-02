package org.hiraeth.govern.server.entity;

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
    private String targetNodeId;

    public Vote(int round, String targetNodeId){
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
        writeStr(targetNodeId);
    }

    public ByteBuffer toBuffer() {
        int strLength = MessageBase.getStrLength(targetNodeId);
        return convertToBuffer(8 + strLength);
    }

    public static Vote parseFrom(MessageBase messageBase) {
        Vote vote = BeanUtil.copyProperties(messageBase, Vote.class);
        ByteBuffer buffer = messageBase.getBuffer();
        vote.round = buffer.getInt();
        vote.targetNodeId = messageBase.readStr();
        return vote;
    }
}
