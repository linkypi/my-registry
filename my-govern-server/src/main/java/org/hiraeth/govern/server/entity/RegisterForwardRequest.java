package org.hiraeth.govern.server.entity;

import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.util.CommonUtil;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/4 11:04
 */
@Getter
@Setter
public class RegisterForwardRequest extends ClusterBaseMessage{

    private String serviceName;
    /**
     * 服务实例IP
     */
    private String instanceIp;
    /**
     * 服务实例端口
     */
    private int servicePort;

    public RegisterForwardRequest(){}

    public RegisterForwardRequest(String serviceName, String instanceIp, int servicePort) {
        super();
        this.instanceIp = instanceIp;
        this.servicePort = servicePort;
        this.serviceName = serviceName;
        this.setClusterMessageType(ClusterMessageType.RegisterForward);
    }

    public ClusterMessage toMessage(){
        ByteBuffer buffer = toBuffer();
        return new ClusterMessage(clusterMessageType, buffer.array());
    }

    public ByteBuffer toBuffer() {
        int length = CommonUtil.getJsonStringLength(serviceName) +
        CommonUtil.getJsonStringLength(instanceIp);
        return super.convertToBuffer(12 + length);
    }

    @Override
    protected void writePayload(ByteBuffer buffer){
        buffer.putInt(servicePort);
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeStr(buffer, instanceIp);
    }
    public static RegisterForwardRequest parseFrom(ClusterBaseMessage messageBase) {
        ByteBuffer buffer = messageBase.getBuffer();
        int port = buffer.getInt();
        String name = CommonUtil.readStr(buffer);
        String ip = CommonUtil.readStr(buffer);
        return new RegisterForwardRequest(name, ip, port);
    }
}
