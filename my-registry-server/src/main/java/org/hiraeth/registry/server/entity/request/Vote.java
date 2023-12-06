package org.hiraeth.registry.server.entity.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.util.CommonUtil;
import org.hiraeth.registry.server.entity.ServerRequestType;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 19:27
 */
@Getter
@Setter
public class Vote extends RequestMessage {
    /**
     * 投票轮次
     */
    private int round;

    /**
     * 投票给哪个controller节点
     */
    private String targetNodeId;

    public Vote(int round, String targetNodeId){
        super();
        this.round = round;
        this.targetNodeId = targetNodeId;
        this.requestType = ServerRequestType.Vote.getValue();
    }

    public void buildBuffer() {
        int strLength = CommonUtil.getStrLength(targetNodeId);
        super.buildBuffer(strLength + 8);
    }

    protected void writePayload(ByteBuffer buffer){
        buffer.putInt(round);
        writeStr(targetNodeId);
    }

    public ByteBuffer toBuffer() {
        int strLength = CommonUtil.getStrLength(targetNodeId);
        return toBuffer(8 + strLength);
    }

    public static Vote parseFrom(RequestMessage messageBase) {
        Vote vote = BeanUtil.copyProperties(messageBase, Vote.class);
        ByteBuffer buffer = messageBase.getBuffer();
        vote.round = buffer.getInt();
        vote.targetNodeId = messageBase.readStr();
        return vote;
    }
}
