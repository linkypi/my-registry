package org.hiraeth.govern.server.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.core.NodeStatusManager;
import org.hiraeth.govern.server.node.core.ElectionStage;

import java.nio.ByteBuffer;

/**
 * 服务端集群内部通信基础组件
 * @author: lynch
 * @description:
 * @date: 2023/11/30 13:58
 */
@Getter
@Setter
public class ClusterBaseMessage {
    protected ClusterMessageType clusterMessageType;
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

    public ClusterBaseMessage(ClusterMessageType clusterMessageType, String controllerId, int epoch){

        this.clusterMessageType = clusterMessageType;
        this.timestamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.stage = ElectionStage.ELStage.LEADING.getValue();
    }

    public ClusterMessage toMessage(){
        ByteBuffer buffer = convertToBuffer(0);
        return new ClusterMessage(clusterMessageType, buffer.array());
    }

    public ClusterBaseMessage(){
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
        buffer.putInt(clusterMessageType.getValue());
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

    public static ClusterBaseMessage parseFromBuffer(ByteBuffer buffer) {
        int requestType = buffer.getInt();
        ClusterMessageType msgType = ClusterMessageType.of(requestType);

        ClusterBaseMessage messageBase = new ClusterBaseMessage();
        messageBase.setClusterMessageType(msgType);
        messageBase.controllerId = CommonUtil.readStr(buffer);
        messageBase.epoch = buffer.getInt();
        messageBase.timestamp = buffer.getLong();
        messageBase.fromNodeId = CommonUtil.readStr(buffer);
        messageBase.stage = buffer.getInt();
        messageBase.buffer = buffer;
        return messageBase;
    }

}
