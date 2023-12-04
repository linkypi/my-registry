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
public class HeartbeatForwardRequest extends ClusterBaseMessage{

    private String serviceName;
    /**
     * 服务实例IP
     */
    private String serviceInstanceIp;
    /**
     * 服务实例端口
     */
    private int serviceInstancePort;

    public HeartbeatForwardRequest(String serviceName, String serviceInstanceIp, int serviceInstancePort) {
        super();
        this.serviceInstanceIp = serviceInstanceIp;
        this.serviceInstancePort = serviceInstancePort;
        this.serviceName = serviceName;
        this.setClusterMessageType(ClusterMessageType.RegisterForward);
    }

    public ClusterMessage toMessage(){
        ByteBuffer buffer = toBuffer();
        return new ClusterMessage(clusterMessageType, buffer.array());
    }

    public ByteBuffer toBuffer() {
        int length = CommonUtil.getJsonStringLength(serviceName) +
        CommonUtil.getJsonStringLength(serviceInstanceIp);
        return super.convertToBuffer(12 + length);
    }

    @Override
    protected void writePayload(ByteBuffer buffer){
        buffer.putInt(serviceInstancePort);
        CommonUtil.writeStr(buffer, serviceName);
        CommonUtil.writeStr(buffer, serviceInstanceIp);
    }
    public static HeartbeatForwardRequest parseFrom(ClusterBaseMessage messageBase) {
        ByteBuffer buffer = messageBase.getBuffer();
        int port = buffer.getInt();
        String name = CommonUtil.readStr(buffer);
        String ip = CommonUtil.readStr(buffer);
        return new HeartbeatForwardRequest(name, ip, port);
    }
}
