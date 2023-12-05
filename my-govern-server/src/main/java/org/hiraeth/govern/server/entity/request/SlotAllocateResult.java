package org.hiraeth.govern.server.entity.request;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.SlotRange;
import org.hiraeth.govern.common.domain.SlotReplica;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.server.entity.ServerRequestType;

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
public class SlotAllocateResult extends RequestMessage {

    public SlotAllocateResult(){}
    private Map<String, List<SlotRange>> slots;
    private Map<String, List<SlotReplica>> slotReplicas;

    public SlotAllocateResult(Map<String, List<SlotRange>> slots, Map<String, List<SlotReplica>> slotReplicas) {
        super();
        this.requestType = ServerRequestType.AllocateSlots.getValue();
        this.slots = slots;
        this.slotReplicas = slotReplicas;
    }

    @Override
    protected void writePayload(ByteBuffer buffer) {
        CommonUtil.writeJsonString(buffer, slots);
        CommonUtil.writeJsonString(buffer, slotReplicas);
    }

    public void buildBuffer(){
        toBuffer();
    }

    public ByteBuffer toBuffer() {
        int length = CommonUtil.getJsonStringLength(slots) +
                CommonUtil.getJsonStringLength(slotReplicas);
        return super.toBuffer(length + 8);
    }

    public static SlotAllocateResult parseFrom(RequestMessage messageBase) {

        ByteBuffer buffer = messageBase.getBuffer();
        String json = CommonUtil.readStr(buffer);
        Map<String, List<SlotRange>> slotRangMap = new HashMap<>();
        Map<String, JSONArray> sourceMap = (Map) JSON.parse(json);
        for (String item : sourceMap.keySet()) {
            String jsonArr = sourceMap.get(item).toString();
            List<SlotRange> slotRanges = JSON.parseArray(jsonArr, SlotRange.class);
            slotRangMap.put(item, slotRanges);
        }

        String jsonStr = CommonUtil.readStr(buffer);
        Map<String, JSONArray> jsonObjectMap = (Map) JSON.parse(jsonStr);

        Map<String, List<SlotReplica>> replicasMap = new HashMap<>();
        for (String nodeId : jsonObjectMap.keySet()) {
            String jsonArr = jsonObjectMap.get(nodeId).toString();
            List<SlotReplica> replicas = JSON.parseArray(jsonArr, SlotReplica.class);
            replicasMap.put(nodeId, replicas);
        }

        SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slotRangMap, replicasMap);
        slotAllocateResult.setControllerId(messageBase.getControllerId());
        slotAllocateResult.setTimestamp(messageBase.getTimestamp());
        slotAllocateResult.setEpoch(messageBase.getEpoch());
        slotAllocateResult.setFromNodeId(messageBase.getFromNodeId());
        return slotAllocateResult;
    }

}
