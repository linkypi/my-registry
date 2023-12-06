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
 * @date: 2023/12/2 12:29
 */
@Getter
@Setter
public class RegisterServiceRequest extends Request {

    private String serviceName;
    /**
     * 服务实例IP
     */
    private String instanceIp;
    /**
     * 服务实例端口
     */
    private int servicePort;

    public RegisterServiceRequest() {
        super();
        this.requestType = RequestType.RegisterService.getValue();
        this.requestId = SnowFlakeIdUtil.getNextId();
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    protected void writePayload() {
        buffer.putInt(servicePort);
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeStr(buffer, instanceIp);
    }

    public void buildBuffer() {
        int length = 12 + CommonUtil.getStrLength(serviceName) + CommonUtil.getStrLength(instanceIp);
        buildBufferInternal(length);
    }

    private static RegisterServiceRequest getRegisterServiceRequest(ByteBuffer buffer, Request request) {
        int port = buffer.getInt();
        String serviceName = CommonUtil.readStr(buffer);
        String instanceIp = CommonUtil.readStr(buffer);
        RegisterServiceRequest registerServiceRequest = BeanUtil.copyProperties(request, RegisterServiceRequest.class);
        registerServiceRequest.servicePort = port;
        registerServiceRequest.serviceName = serviceName;
        registerServiceRequest.instanceIp = instanceIp;
        return registerServiceRequest;
    }

    public static RegisterServiceRequest parseFrom(Request request) {
        ByteBuffer buffer = request.getBuffer();
        return getRegisterServiceRequest(buffer, request);
    }
}
