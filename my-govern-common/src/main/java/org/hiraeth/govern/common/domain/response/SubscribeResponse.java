package org.hiraeth.govern.common.domain.response;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.RequestType;
import org.hiraeth.govern.common.domain.ServiceInstanceInfo;
import org.hiraeth.govern.common.domain.request.SubscribeRequest;
import org.hiraeth.govern.common.util.CommonUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 20:27
 */
@Getter
@Setter
public class SubscribeResponse extends Response{

    private List<ServiceInstanceInfo> serviceInstanceInfoAddresses;

    private SubscribeResponse(){
        super();
    }
    public SubscribeResponse(SubscribeRequest request){
        super();
        this.requestType = RequestType.Subscribe;
        this.requestId = request.getRequestId();
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    protected void writePayload(){
        // 因为 writePayload 已重写 Response 故必须写入 Response 的所有属性 success
        buffer.putInt(success ? 1 : 0);
        // 写入 serviceInstanceAddresses
        CommonUtil.writeJsonString(buffer, serviceInstanceInfoAddresses);
    }

    public void buildBuffer() {
        int length = 8 + CommonUtil.getJsonStringLength(serviceInstanceInfoAddresses);
        buildBufferInternal(length);
    }

    public static SubscribeResponse parseFrom(Response baseResponse) {

        ByteBuffer buffer = baseResponse.getBuffer();
        String jsonStr = CommonUtil.readStr(buffer);
        List<ServiceInstanceInfo> serviceInstanceInfos = JSON.parseArray(jsonStr, ServiceInstanceInfo.class);

        SubscribeResponse subscribeResponse = new SubscribeResponse();
        subscribeResponse.setSuccess(baseResponse.isSuccess());
        subscribeResponse.setRequestId(baseResponse.getRequestId());
        subscribeResponse.setRequestType(baseResponse.getRequestType());
        subscribeResponse.setTimestamp(baseResponse.getTimestamp());
        subscribeResponse.serviceInstanceInfoAddresses = serviceInstanceInfos;
        return subscribeResponse;
    }
}
