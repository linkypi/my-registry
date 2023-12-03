package org.hiraeth.govern.common.domain.request;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.RequestType;
import org.hiraeth.govern.common.domain.ServiceInstanceInfo;
import org.hiraeth.govern.common.util.CommonUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/3 13:42
 */
@Getter
@Setter
public class NotifySubscribeRequest extends Request{
    private String serviceName;
    private List<ServiceInstanceInfo> serviceInstanceInfoAddresses;
    public NotifySubscribeRequest(String serviceName, List<ServiceInstanceInfo> serviceInstanceInfoAddresses){
        super();
        this.serviceName = serviceName;
        this.requestType = RequestType.NotifySubscribe;
        this.serviceInstanceInfoAddresses = serviceInstanceInfoAddresses;
    }

    public void buildBuffer() {
        int length = 8 + CommonUtil.getStrLength(serviceName) + CommonUtil.getJsonStringLength(serviceInstanceInfoAddresses);
        buildBufferInternal(length);
    }

    @Override
    protected void writePayload() {
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeJsonString(buffer, serviceInstanceInfoAddresses);
    }

    public static NotifySubscribeRequest parseFrom(Request request) {

        ByteBuffer buffer = request.getBuffer();
        String serviceName = CommonUtil.readStr(buffer);
        String jsonStr = CommonUtil.readStr(buffer);
        List<ServiceInstanceInfo> serviceInstanceInfos = JSON.parseArray(jsonStr, ServiceInstanceInfo.class);

        NotifySubscribeRequest subscribeRequest = new NotifySubscribeRequest(serviceName, serviceInstanceInfos);
        subscribeRequest.setRequestId(request.getRequestId());
        subscribeRequest.setRequestType(request.getRequestType());
        subscribeRequest.setTimestamp(request.getTimestamp());
        return subscribeRequest;
    }

}
