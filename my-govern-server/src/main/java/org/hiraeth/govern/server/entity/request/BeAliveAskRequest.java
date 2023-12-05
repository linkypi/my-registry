package org.hiraeth.govern.server.entity.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.server.entity.ServerRequestType;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/5 10:57
 */
@Slf4j
@Getter
@Setter
public class BeAliveAskRequest extends RequestMessage {

    @Getter
    public enum Status{
        Alive(1),
        Down(2);
        Status(int val){
            this.value = val;
        }
        private int value;
    }

    // 询问节点是否存活
    private String remoteNodeId;

    public BeAliveAskRequest() {
        super();
        this.requestType = ServerRequestType.BeAliveAsk.getValue();
    }

    public BeAliveAskRequest(String remoteNodeId) {
        super();
        this.requestType = ServerRequestType.BeAliveAsk.getValue();
        this.remoteNodeId = remoteNodeId;
    }

    public void buildBuffer(){
        super.toBuffer(4 + CommonUtil.getStrLength(remoteNodeId));
    }

    @Override
    protected void writePayload(ByteBuffer buffer){
        writeStr(remoteNodeId);
    }

    public ByteBuffer toBuffer(){
        return super.toBuffer(4);
    }

    public static BeAliveAskRequest parseFrom(RequestMessage messageBase) {
        ByteBuffer buffer = messageBase.getBuffer();
        String nodeId = CommonUtil.readStr(buffer);
        BeAliveAskRequest beAliveAskRequest = BeanUtil.copyProperties(messageBase, BeAliveAskRequest.class);
        beAliveAskRequest.setRemoteNodeId(nodeId);
        return beAliveAskRequest;
    }
}
