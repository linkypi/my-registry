package org.hiraeth.registry.server.entity.request;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.util.CommonUtil;
import org.hiraeth.registry.server.entity.ServerRequestType;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/4 11:04
 */
@Getter
@Setter
public class RegisterForwardRequest extends RequestMessage {

    private String serviceName;
    /**
     * 服务实例IP
     */
    private String instanceIp;
    /**
     * 服务实例端口
     */
    private int servicePort;

    public RegisterForwardRequest(){
        super();
        this.setRequestType(ServerRequestType.RegisterForward.getValue());
    }

    public RegisterForwardRequest(String serviceName, String instanceIp, int servicePort) {
        super();
        this.instanceIp = instanceIp;
        this.servicePort = servicePort;
        this.serviceName = serviceName;
        this.setRequestType(ServerRequestType.RegisterForward.getValue());
    }

    public void buildBuffer(){
        toBuffer();
    }

    public ByteBuffer toBuffer() {
        int length = CommonUtil.getJsonStringLength(serviceName) +
        CommonUtil.getJsonStringLength(instanceIp);
        return super.toBuffer(12 + length);
    }

    @Override
    protected void writePayload(ByteBuffer buffer){
        buffer.putInt(servicePort);
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeStr(buffer, instanceIp);
    }
    public static RegisterForwardRequest parseFrom(RequestMessage messageBase) {
        ByteBuffer buffer = messageBase.getBuffer();
        int port = buffer.getInt();
        String name = CommonUtil.readStr(buffer);
        String ip = CommonUtil.readStr(buffer);
        return new RegisterForwardRequest(name, ip, port);
    }
}
