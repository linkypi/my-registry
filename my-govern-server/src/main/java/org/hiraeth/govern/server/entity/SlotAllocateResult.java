package org.hiraeth.govern.server.entity;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.SlotRange;
import org.hiraeth.govern.common.util.CommonUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 10:54
 */
@Getter
@Setter
public class SlotAllocateResult extends ClusterBaseMessage {

    public SlotAllocateResult(){}
    private Map<String, SlotRange> slots;
    private Map<String,Map<String, List<SlotRange>>> slotReplicas;

    public SlotAllocateResult(Map<String, SlotRange> slots, Map<String,Map<String, List<SlotRange>>> slotReplicas) {
        super();
        this.clusterMessageType = ClusterMessageType.AllocateSlots;
        this.slots = slots;
        this.slotReplicas = slotReplicas;
    }

    @Override
    protected void writePayload(ByteBuffer buffer) {
        CommonUtil.writeJsonString(buffer, slots);
        CommonUtil.writeJsonString(buffer, slotReplicas);
    }

    public ByteBuffer toBuffer() {
        int length = CommonUtil.getJsonStringLength(slots) +
                CommonUtil.getJsonStringLength(slotReplicas);
        return super.convertToBuffer(length + 8);
    }

    public ClusterMessage toMessage() {
        ByteBuffer buffer = toBuffer();
        return new ClusterMessage(clusterMessageType, buffer.array());
    }

    public static SlotAllocateResult parseFrom(ClusterBaseMessage messageBase) {

        ByteBuffer buffer = messageBase.getBuffer();
        String json = CommonUtil.readStr(buffer);
        Map<String, SlotRange> slotRangMap = new HashMap<>();
        Map<String, JSONObject> sourceMap = (Map) JSON.parse(json);
        for (String item : sourceMap.keySet()) {
            JSONObject jsonObject = sourceMap.get(item);
            SlotRange slotRange = JSON.parseObject(jsonObject.toJSONString(), SlotRange.class);
            slotRangMap.put(item, slotRange);
        }

        String jsonStr = CommonUtil.readStr(buffer);
        Map<String, JSONObject> jsonObjectMap = (Map) JSON.parse(jsonStr);

        Map<String, Map<String, List<SlotRange>>> replicasMap = new HashMap<>();
        for (String item : jsonObjectMap.keySet()) {
            Map<String, List<SlotRange>> jsonObject = (Map) jsonObjectMap.get(item);
            Map<String, List<SlotRange>> temp = new HashMap<>();
            for (String key : jsonObject.keySet()) {
                List<SlotRange> ranges = JSON.parseArray(jsonObject.get(key).toString(), SlotRange.class);
                temp.put(key, ranges);
                replicasMap.put(item, temp);
            }
        }

        SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slotRangMap, replicasMap);
        slotAllocateResult.setControllerId(messageBase.getControllerId());
        slotAllocateResult.setTimestamp(messageBase.getTimestamp());
        slotAllocateResult.setEpoch(messageBase.getEpoch());
        slotAllocateResult.setFromNodeId(messageBase.getFromNodeId());
        return slotAllocateResult;
    }

}
