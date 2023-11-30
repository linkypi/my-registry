package org.hiraeth.govern.server.node.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 12:38
 */
@Getter
@Setter
public class SlotAllocateResultAck {
    private int controllerId;
    private int epoch;
    private long timeStamp;
    private int fromNodeId;

    public SlotAllocateResultAck(){
        NodeStatusManager statusManager = NodeStatusManager.getInstance();
        this.controllerId = statusManager.getControllerId();
        this.epoch = statusManager.getEpoch();
        this.timeStamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
    }

    public SlotAllocateResultAck(int controllerId, int epoch, long timeStamp, int fromNodeId) {
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.timeStamp = timeStamp;
        this.fromNodeId = fromNodeId;
    }

    public ByteBuffer toBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putInt(MessageType.AllocateSlots.getValue());
        buffer.putInt(controllerId);
        buffer.putInt(epoch);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(fromNodeId);
        return buffer;
    }

    public static SlotAllocateResultAck parseFrom(ByteBuffer buffer) {
        int controllerId = buffer.getInt();
        int epoch = buffer.getInt();
        long timeStamp = buffer.getLong();
        int from = buffer.getInt();
        return new SlotAllocateResultAck(controllerId, epoch, timeStamp, from);
    }

}
