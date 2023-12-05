package org.hiraeth.govern.common.domain.response;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.RequestType;
import org.hiraeth.govern.common.domain.ServerAddress;
import org.hiraeth.govern.common.domain.SlotRange;
import org.hiraeth.govern.common.util.CommonUtil;

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
public class FetchMetaDataResponse extends Response {

    private Map<String, List<SlotRange>> slots;
    private List<ServerAddress> serverAddresses;

    public FetchMetaDataResponse() {
        super();
        timestamp = System.currentTimeMillis();
        this.requestType = RequestType.FetchMetaData.getValue();
    }

    @Override
    protected void writePayload() {
        // 因为 writePayload 已重写 Response 故必须写入 Response 的所有属性 success
        buffer.putInt(success ? 1 : 0);
        // 写入 slots
        CommonUtil.writeJsonString(buffer, slots);

        // 写入 master addresses
        CommonUtil.writeJsonString(buffer, serverAddresses);
    }

    public void buildBuffer() {
        int length = 12 + CommonUtil.getJsonStringLength(slots) + CommonUtil.getJsonStringLength(serverAddresses);
        buildBufferInternal(length);
    }

    public static FetchMetaDataResponse parseFrom(Response baseResponse) {

        ByteBuffer buffer = baseResponse.getBuffer();

        // 先读取 slots 数据
        int slotsLength = buffer.getInt();
        byte[] bytes = new byte[slotsLength];
        buffer.get(bytes);
        String json = new String(bytes);

        Map<String, List<SlotRange>> slotRangMap = new HashMap<>();
        Map<String, JSONArray> sourceMap = (Map) JSON.parse(json);
        for (String item : sourceMap.keySet()) {
            String jsonArr = sourceMap.get(item).toString();
            List<SlotRange> ranges = JSON.parseArray(jsonArr, SlotRange.class);
            slotRangMap.put(item, ranges);
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
