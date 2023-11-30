package org.hiraeth.govern.server.node.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.master.ElectionStage;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 13:58
 */
@Getter
@Setter
public class MessageBase {
    protected MessageType messageType;
    protected int controllerId;
    protected int epoch;
    protected long timestamp;
    protected Integer fromNodeId;

    //       // 选举阶段
    //        ELECTING 1,
    //        // 候选阶段, 已有初步投票结果, 需进一步确认
    //        CANDIDATE 2,
    //        // 领导阶段, 即已选举产生 leader
    //        LEADING 3
    protected int stage;

    private ByteBuffer buffer;

    public MessageBase(){
        NodeStatusManager statusManager = NodeStatusManager.getInstance();
        this.timestamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
        this.controllerId = statusManager.getControllerId();
        this.epoch = statusManager.getEpoch();
        this.stage = statusManager.getStage().getValue();
    }

    protected void writePayload(ByteBuffer buffer){
    }

    protected ByteBuffer newBuffer(int payloadLength) {
        ByteBuffer buffer = ByteBuffer.allocate(28 + payloadLength);
        buffer.putInt(messageType.getValue());
        buffer.putInt(controllerId);
        buffer.putInt(epoch);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(fromNodeId);
        buffer.putInt(ElectionStage.getStatus().getValue());
        writePayload(buffer);
        return buffer;
    }

    public static MessageBase parseFromBuffer(ByteBuffer buffer) {
        int requestType = buffer.getInt();
        MessageType msgType = MessageType.of(requestType);

        MessageBase messageBase = new MessageBase();
        messageBase.setMessageType(msgType);
        messageBase.controllerId = buffer.getInt();
        messageBase.epoch = buffer.getInt();
        messageBase.timestamp = buffer.getLong();
        messageBase.fromNodeId = buffer.getInt();
        messageBase.stage = buffer.getInt();
        messageBase.buffer = buffer;
        return messageBase;
    }

}
