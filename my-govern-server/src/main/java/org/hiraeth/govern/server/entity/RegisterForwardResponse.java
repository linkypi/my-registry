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
public class RegisterForwardResponse extends ClusterBaseMessage{

    private boolean success;

    public RegisterForwardResponse(){}

    public RegisterForwardResponse(boolean success) {
        super();
        this.success = success;
        this.setClusterMessageType(ClusterMessageType.RegisterForward);
    }

    public ClusterMessage toMessage(){
        ByteBuffer buffer = toBuffer();
        return new ClusterMessage(clusterMessageType, buffer.array());
    }

    public ByteBuffer toBuffer() {
        return super.convertToBuffer(4);
    }

    @Override
    protected void writePayload(ByteBuffer buffer) {
        buffer.putInt(success ? 1 : 0);
    }
    public static RegisterForwardResponse parseFrom(ClusterBaseMessage messageBase) {
        ByteBuffer buffer = messageBase.getBuffer();
        int flag = buffer.getInt();
        return new RegisterForwardResponse(flag == 1);
    }
}
