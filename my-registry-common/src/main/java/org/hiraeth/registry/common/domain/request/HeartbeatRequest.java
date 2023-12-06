package org.hiraeth.registry.common.domain.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.RequestType;
import org.hiraeth.registry.common.snowflake.SnowFlakeIdUtil;
import org.hiraeth.registry.common.util.CommonUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/2 16:42
 */
@Getter
@Setter
public class HeartbeatRequest extends Request {
    private String serviceName;
    private String serviceInstanceIp;
    private int serviceInstancePort;

    public HeartbeatRequest(){
        super();
        this.requestType = RequestType.Heartbeat.getValue();
        this.requestId = SnowFlakeIdUtil.getNextId();
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    protected void writePayload() {
        buffer.putInt(serviceInstancePort);
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeStr(buffer, serviceInstanceIp);
    }

    public void buildBuffer() {
        int length = 12 + CommonUtil.getStrLength(serviceName) + CommonUtil.getStrLength(serviceInstanceIp);
        buildBufferInternal(length);
    }

    private static HeartbeatRequest getRequest(ByteBuffer buffer, Request request) {
        int port = buffer.getInt();
        String serviceName = CommonUtil.readStr(buffer);
        String instanceIp = CommonUtil.readStr(buffer);
        HeartbeatRequest registerServiceRequest = BeanUtil.copyProperties(request, HeartbeatRequest.class);
        registerServiceRequest.serviceInstancePort = port;
        registerServiceRequest.serviceName = serviceName;
        registerServiceRequest.serviceInstanceIp = instanceIp;
        return registerServiceRequest;
    }

    public static HeartbeatRequest parseFrom(Request request) {
        ByteBuffer buffer = request.getBuffer();
        return getRequest(buffer, request);
    }
}
