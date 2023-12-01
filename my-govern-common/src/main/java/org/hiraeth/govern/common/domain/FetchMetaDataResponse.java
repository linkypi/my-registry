package org.hiraeth.govern.common.domain;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:30
 */
@Getter
@Setter
public class FetchMetaDataResponse extends BaseResponse {

    private Map<String, SlotRang> slots;
    private List<ServerAddress> serverAddresses;

    public FetchMetaDataResponse() {
        timestamp = System.currentTimeMillis();
        this.requestType = RequestType.FetchMetaData;
    }

    public Response toResponse() {
        byte[] bytes = JSON.toJSONString(slots).getBytes();
        byte[] addresses = JSON.toJSONString(serverAddresses).getBytes();
        // 仍需加上8字节长度， 因为每个属性有4个字节数据长度
        toBuffer(bytes.length + addresses.length + 8);
        return new Response(requestType, requestId, buffer);
    }

    @Override
    protected void writePayload(){
        // 写入 slots
        byte[] bytes = JSON.toJSONString(slots).getBytes();
        buffer.putInt(bytes.length);
        buffer.put(bytes);

        // 写入 master addresses
        byte[] addresses = JSON.toJSONString(serverAddresses).getBytes();
        buffer.putInt(addresses.length);
        buffer.put(addresses);
    }

    public static FetchMetaDataResponse parseFrom(BaseResponse baseResponse) {

        ByteBuffer buffer = baseResponse.getBuffer();

        // 先读取 slots 数据
        int slotsLength = buffer.getInt();
        byte[] bytes = new byte[slotsLength];
        buffer.get(bytes);
        String json = new String(bytes);

        Map<String, SlotRang> slotRangMap = new HashMap<>();
        Map<String, JSONObject> sourceMap = (Map) JSON.parse(json);
        for (String item : sourceMap.keySet()) {
            JSONObject jsonObject = sourceMap.get(item);
            SlotRang slotRang = new SlotRang(jsonObject.getIntValue("start"), jsonObject.getIntValue("end"));
            slotRangMap.put(item, slotRang);
        }

        FetchMetaDataResponse response = BeanUtil.copyProperties(baseResponse, FetchMetaDataResponse.class);
        response.setSlots(slotRangMap);

        // 再读取 addresses
        int addrLength = buffer.getInt();
        byte[] addrBytes = new byte[addrLength];
        buffer.get(addrBytes);
        String addrJson = new String(addrBytes);
        List<ServerAddress> addressList = JSON.parseArray(addrJson, ServerAddress.class);
        response.setServerAddresses(addressList);
        return response;
    }
}
