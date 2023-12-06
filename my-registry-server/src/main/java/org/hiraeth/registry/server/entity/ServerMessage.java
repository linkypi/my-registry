package org.hiraeth.registry.server.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.Message;
import org.hiraeth.registry.common.domain.MessageType;
import org.hiraeth.registry.common.snowflake.SnowFlakeIdUtil;
import org.hiraeth.registry.common.util.CommonUtil;
import org.hiraeth.registry.server.config.Configuration;
import org.hiraeth.registry.server.node.core.NodeInfoManager;
import org.hiraeth.registry.server.node.core.ElectionStage;

import java.nio.ByteBuffer;

/**
 * 服务端集群内部通信基础组件
 * @author: lynch
 * @description:
 * @date: 2023/11/30 13:58
 */
@Getter
@Setter
public class ServerMessage extends Message {
    protected String controllerId;
    protected int epoch;
    protected String fromNodeId;
    // 发送到的节点id
    protected String toNodeId;

    //  选举阶段
    //  ELECTING 1,
    //  领导阶段, 即已选举产生 leader
    //  LEADING 3
    protected int stage;

    // 仅用作停止写线程标识，不做远程传输
    private boolean terminated;

    public ServerMessage(ServerRequestType requestType, String controllerId, int epoch) {

        this.requestType = requestType.getValue();
        this.timestamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
        this.requestId = SnowFlakeIdUtil.getNextId();
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.stage = ElectionStage.ELStage.LEADING.getValue();
    }

    public static ServerMessage newRequestMessage(ServerRequestType requestType) {
        ServerMessage serverMessage = new ServerMessage();
        serverMessage.requestType = requestType.getValue();
        serverMessage.messageType = MessageType.REQUEST;
        return serverMessage;
    }

    public static ServerMessage newResponseMessage(ServerRequestType requestType) {
        ServerMessage serverMessage = new ServerMessage();
        serverMessage.requestType = requestType.getValue();
        serverMessage.messageType = MessageType.RESPONSE;
        return serverMessage;
    }

    public ServerMessage(boolean terminated) {
        this.terminated = terminated;
    }

    public ServerMessage() {
        NodeInfoManager statusManager = NodeInfoManager.getInstance();
        this.timestamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
        this.controllerId = statusManager.getControllerId();
        this.epoch = statusManager.getEpoch();
        this.stage = statusManager.getStage().getValue();
        this.requestId = SnowFlakeIdUtil.getNextId();
    }

    protected void writePayload(ByteBuffer buffer) {
    }

    protected ByteBuffer toBuffer(int payloadLength) {

        if (messageType == null) {
            throw new IllegalArgumentException("message type not specified.");
        }
        if (requestType == 0) {
            throw new IllegalArgumentException("request type not specified.");
        }
        int strLength = CommonUtil.getStrLength(controllerId) +
                CommonUtil.getStrLength(fromNodeId) + CommonUtil.getStrLength(toNodeId);

        buffer = ByteBuffer.allocate(44 + strLength + payloadLength);
        buffer.putInt(messageType.getValue());
        buffer.putInt(requestType);
        // requestId
        buffer.putLong(requestId);
        buffer.putLong(timestamp);

        writeStr(controllerId);
        buffer.putInt(epoch);

        writeStr(fromNodeId);
        writeStr(toNodeId);
        // stage
        buffer.putInt(ElectionStage.getStatus().getValue());

        writePayload(buffer);
        return buffer;
    }

    protected void writeStr(String val) {
        CommonUtil.writeStr(buffer, val);
    }

    public String readStr() {
        return CommonUtil.readStr(buffer);
    }

    protected void writeBoolean(boolean val) {
        buffer.putInt(val ? 1 : 0);
    }

    public static ServerMessage parseFromBuffer(ByteBuffer buffer) {
        int messageType = buffer.getInt();
        int requestType = buffer.getInt();
        MessageType msgType = MessageType.of(messageType);

        ServerMessage messageBase = new ServerMessage();
        messageBase.setMessageType(msgType);
        messageBase.setRequestType(requestType);
        messageBase.requestId = buffer.getLong();
        messageBase.timestamp = buffer.getLong();

        messageBase.controllerId = CommonUtil.readStr(buffer);
        messageBase.epoch = buffer.getInt();

        messageBase.fromNodeId = CommonUtil.readStr(buffer);
        messageBase.toNodeId = CommonUtil.readStr(buffer);
        messageBase.stage = buffer.getInt();

        messageBase.buffer = buffer;
        return messageBase;
    }

}
