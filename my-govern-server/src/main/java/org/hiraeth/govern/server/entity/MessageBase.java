package org.hiraeth.govern.server.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.common.util.StringUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.core.NodeStatusManager;
import org.hiraeth.govern.server.core.ElectionStage;

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
    protected String controllerId;
    protected int epoch;
    protected long timestamp;
    protected String fromNodeId;

    //       // 选举阶段
    //        ELECTING 1,
    //        // 候选阶段, 已有初步投票结果, 需进一步确认
    //        CANDIDATE 2,
    //        // 领导阶段, 即已选举产生 leader
    //        LEADING 3
    protected int stage;

    private ByteBuffer buffer;

    public MessageBase(MessageType messageType, String controllerId, int epoch){

        this.messageType = messageType;
        this.timestamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.stage = ElectionStage.ELStage.LEADING.getValue();
    }

    public Message toMessage(){
        ByteBuffer buffer = convertToBuffer(0);
        return new Message(messageType, buffer.array());
    }

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

    protected ByteBuffer convertToBuffer(int payloadLength) {
        int strLength = CommonUtil.getStrLength(controllerId) + CommonUtil.getStrLength(fromNodeId);
        buffer = ByteBuffer.allocate(28 + strLength + payloadLength);
        buffer.putInt(messageType.getValue());
        writeStr(controllerId);
        buffer.putInt(epoch);
        buffer.putLong(System.currentTimeMillis());
        writeStr(fromNodeId);
        buffer.putInt(ElectionStage.getStatus().getValue());
        writePayload(buffer);
        return buffer;
    }

    protected void writeStr(String val){
        CommonUtil.writeStr(buffer, val);
    }


    protected String readStr() {
        return CommonUtil.readStr(buffer);
    }

    public static MessageBase parseFromBuffer(ByteBuffer buffer) {
        int requestType = buffer.getInt();
        MessageType msgType = MessageType.of(requestType);

        MessageBase messageBase = new MessageBase();
        messageBase.setMessageType(msgType);
        messageBase.controllerId = CommonUtil.readStr(buffer);
        messageBase.epoch = buffer.getInt();
        messageBase.timestamp = buffer.getLong();
        messageBase.fromNodeId = CommonUtil.readStr(buffer);
        messageBase.stage = buffer.getInt();
        messageBase.buffer = buffer;
        return messageBase;
    }

}
