package org.hiraeth.govern.common.domain;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.snowflake.SnowFlakeIdUtil;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:30
 */
@Getter
@Setter
public class FetchSlotsResponse extends BaseResponse {

    private Map<Integer, SlotRang> slots;

    public FetchSlotsResponse() {
        timestamp = System.currentTimeMillis();
        this.requestType = RequestType.FetchSlot;
    }

    public Response toResponse() {
        byte[] bytes = JSON.toJSONString(slots).getBytes();
        toBuffer(bytes.length);
        return new Response(requestType, requestId, buffer);
    }

    @Override
    protected void writePayload(){
        byte[] bytes = JSON.toJSONString(slots).getBytes();
        buffer.put(bytes);
    }

    public static FetchSlotsResponse parseFrom(ByteBuffer buffer) {
        BaseRequest request = BaseRequest.parseFromBuffer(buffer);
        int remind = buffer.remaining();
        byte[] bytes = new byte[remind];
        buffer.get(bytes);
        String json = new String(bytes);

        Map<Integer, SlotRang> slotRangMap = new HashMap<>();
        Map<Integer, JSONObject> sourceMap = (Map) JSON.parse(json);
        for (Integer item : sourceMap.keySet()) {
            JSONObject jsonObject = sourceMap.get(item);
            SlotRang slotRang = new SlotRang(jsonObject.getIntValue("start"), jsonObject.getIntValue("end"));
            slotRangMap.put(item, slotRang);
        }

        FetchSlotsResponse response = BeanUtil.copyProperties(request, FetchSlotsResponse.class);
        response.setSlots(slotRangMap);
        return response;
    }
}
