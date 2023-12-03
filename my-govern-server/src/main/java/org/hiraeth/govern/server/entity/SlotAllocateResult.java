package org.hiraeth.govern.server.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.SlotRang;

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
public class SlotAllocateResult extends ClusterBaseMessage {

    private Map<String, SlotRang> slots;

    public SlotAllocateResult(Map<String, SlotRang> slots) {
        super();
        this.clusterMessageType = ClusterMessageType.AllocateSlots;
        this.slots = slots;
    }

    @Override
    protected void writePayload(ByteBuffer buffer) {
        byte[] bytes = JSON.toJSONString(slots).getBytes();
        buffer.put(bytes);
    }

    public ByteBuffer toBuffer() {
        byte[] bytes = JSON.toJSONString(slots).getBytes();
        return super.convertToBuffer(bytes.length);
    }

    public ClusterMessage toMessage() {
        ByteBuffer buffer = toBuffer();
        return new ClusterMessage(clusterMessageType, buffer.array());
    }

    public static SlotAllocateResult parseFrom(ClusterBaseMessage messageBase) {

        ByteBuffer buffer = messageBase.getBuffer();
        int remind = buffer.remaining();
        byte[] bytes = new byte[remind];
        buffer.get(bytes);
        String json = new String(bytes);

        Map<String, SlotRang> slotRangMap = new HashMap<>();
        Map<String, JSONObject> sourceMap = (Map) JSON.parse(json);
        for (String item : sourceMap.keySet()) {
            JSONObject jsonObject = sourceMap.get(item);
            SlotRang slotRang = new SlotRang(jsonObject.getIntValue("start"), jsonObject.getIntValue("end"));
            slotRangMap.put(item, slotRang);
        }
        SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slotRangMap);
        slotAllocateResult.setControllerId(messageBase.getControllerId());
        slotAllocateResult.setTimestamp(messageBase.getTimestamp());
        slotAllocateResult.setEpoch(messageBase.getEpoch());
        slotAllocateResult.setFromNodeId(messageBase.getFromNodeId());
        return slotAllocateResult;
    }

}
