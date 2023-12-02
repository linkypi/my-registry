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
 * @date: 2023/12/2 12:29
 */
@Getter
@Setter
public class RegisterServiceRequest extends BaseRequest {

    private String serviceName;
    /**
     * 服务实例IP
     */
    private String instanceIp;
    /**
     * 服务实例端口
     */
    private int servicePort;

    public RegisterServiceRequest(){
        this.requestType = RequestType.RegisterService;
        this.requestId = SnowFlakeIdUtil.getNextId();
    }

    public Request toRequest() {
        int length = CommonUtil.getStrLength(serviceName) + CommonUtil.getStrLength(instanceIp);
        toBuffer(12 + length);
        return new Request(requestType, requestId, buffer);
    }

    @Override
    protected void writePayload() {
        buffer.putInt(servicePort);
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeStr(buffer, instanceIp);
    }

    private static RegisterServiceRequest getRegisterServiceRequest(ByteBuffer buffer, BaseRequest request) {
        int port = buffer.getInt();
        String serviceName = CommonUtil.readStr(buffer);
        String instanceIp = CommonUtil.readStr(buffer);
        RegisterServiceRequest registerServiceRequest = BeanUtil.copyProperties(request, RegisterServiceRequest.class);
        registerServiceRequest.servicePort = port;
        registerServiceRequest.serviceName = serviceName;
        registerServiceRequest.instanceIp = instanceIp;
        return registerServiceRequest;
    }

    public static RegisterServiceRequest parseFrom(BaseRequest request) {
        ByteBuffer buffer = request.getBuffer();
        return getRegisterServiceRequest(buffer, request);
    }
}
