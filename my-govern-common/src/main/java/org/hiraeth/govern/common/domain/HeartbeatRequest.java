package org.hiraeth.govern.common.domain;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.snowflake.SnowFlakeIdUtil;
import org.hiraeth.govern.common.util.CommonUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 16:42
 */
@Getter
@Setter
public class HeartbeatRequest extends BaseRequest{
    private String serviceName;
    private String serviceInstanceIp;
    private int serviceInstancePort;
    public HeartbeatRequest(){
        this.requestType = RequestType.Heartbeat;
        this.requestId = SnowFlakeIdUtil.getNextId();
        this.timestamp = System.currentTimeMillis();
    }

    public Request toRequest() {
        int length = CommonUtil.getStrLength(serviceName) + CommonUtil.getStrLength(serviceInstanceIp);
        toBuffer(12 + length);
        return new Request(requestType, requestId, buffer);
    }

    @Override
    protected void writePayload() {
        buffer.putInt(serviceInstancePort);
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeStr(buffer, serviceInstanceIp);
    }

    private static HeartbeatRequest getRegisterServiceRequest(ByteBuffer buffer, BaseRequest request) {
        int port = buffer.getInt();
        String serviceName = CommonUtil.readStr(buffer);
        String instanceIp = CommonUtil.readStr(buffer);
        HeartbeatRequest registerServiceRequest = BeanUtil.copyProperties(request, HeartbeatRequest.class);
        registerServiceRequest.serviceInstancePort = port;
        registerServiceRequest.serviceName = serviceName;
        registerServiceRequest.serviceInstanceIp = instanceIp;
        return registerServiceRequest;
    }

    public static HeartbeatRequest parseFrom(BaseRequest request) {
        ByteBuffer buffer = request.getBuffer();
        return getRegisterServiceRequest(buffer, request);
    }
}
