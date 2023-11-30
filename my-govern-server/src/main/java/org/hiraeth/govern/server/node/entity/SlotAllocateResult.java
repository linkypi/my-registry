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
 * @date: 2023/11/30 10:54
 */
@Getter
@Setter
public class SlotAllocateResult {
    private int controllerId;
    private int epoch;
    private long timeStamp;
    private int fromNodeId;
    private Map<Integer, SlotRang> slots;

    public SlotAllocateResult(Map<Integer, SlotRang> slots){
        NodeStatusManager statusManager = NodeStatusManager.getInstance();
        this.controllerId = statusManager.getControllerId();
        this.slots = slots;
        this.epoch = statusManager.getEpoch();
        this.timeStamp = System.currentTimeMillis();
        this.fromNodeId = Configuration.getInstance().getNodeId();
    }

    public SlotAllocateResult(int controllerId, int epoch, long timeStamp, int fromNodeId, Map<Integer, SlotRang> slots) {
        this.controllerId = controllerId;
        this.slots = slots;
        this.epoch = epoch;
        this.timeStamp = timeStamp;
        this.fromNodeId = fromNodeId;
    }

    public ByteBuffer toBuffer() {
        byte[] bytes = JSON.toJSONString(slots).getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(24 + bytes.length);
        buffer.putInt(MessageType.AllocateSlots.getValue());
        buffer.putInt(controllerId);
        buffer.putInt(epoch);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(fromNodeId);
        buffer.put(bytes);
        return buffer;
    }

    public static SlotAllocateResult parseFrom(ByteBuffer buffer) {
        int controllerId = buffer.getInt();
        int epoch = buffer.getInt();
        long timeStamp = buffer.getLong();
        int from = buffer.getInt();
        int remind = buffer.remaining();
        byte[] bytes = new byte[remind];
        buffer.get(bytes);
        String json = new String(bytes);

        Map<Integer, SlotRang> slotRangMap = new HashMap<>();
        Map<Integer, JSONObject> sourceMap = (Map)JSON.parse(json);
        for (Integer item: sourceMap.keySet()){
            JSONObject jsonObject = sourceMap.get(item);
            SlotRang slotRang = new SlotRang(jsonObject.getIntValue("start"), jsonObject.getIntValue("end"));
            slotRangMap.put(item, slotRang);
        }
        return new SlotAllocateResult(controllerId, epoch, timeStamp, from, slotRangMap);
    }

}
