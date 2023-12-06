package org.hiraeth.registry.common.domain.request;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.RequestType;
import org.hiraeth.registry.common.domain.ServiceInstanceInfo;
import org.hiraeth.registry.common.util.CommonUtil;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/3 13:42
 */
@Getter
@Setter
public class ServiceChangedRequest extends Request{
    private String serviceName;
    private List<ServiceInstanceInfo> serviceInstanceInfoAddresses;
    public ServiceChangedRequest(String serviceName, List<ServiceInstanceInfo> serviceInstanceInfoAddresses){
        super();
        this.serviceName = serviceName;
        this.requestType = RequestType.NotifySubscribe.getValue();
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

    public static ServiceChangedRequest parseFrom(Request request) {

        ByteBuffer buffer = request.getBuffer();
        String serviceName = CommonUtil.readStr(buffer);
        String jsonStr = CommonUtil.readStr(buffer);
        List<ServiceInstanceInfo> serviceInstanceInfos = JSON.parseArray(jsonStr, ServiceInstanceInfo.class);

        ServiceChangedRequest subscribeRequest = new ServiceChangedRequest(serviceName, serviceInstanceInfos);
        subscribeRequest.setRequestId(request.getRequestId());
        subscribeRequest.setRequestType(request.getRequestType());
        subscribeRequest.setTimestamp(request.getTimestamp());
        return subscribeRequest;
    }

}
