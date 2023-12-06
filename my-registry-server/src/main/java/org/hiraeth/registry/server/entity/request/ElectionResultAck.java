package org.hiraeth.registry.server.entity.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.server.entity.ServerRequestType;

import java.nio.ByteBuffer;

/**
 *
 * @author: lynch
 * @description:
 * @date: 2023/11/29 14:02
 */
@Slf4j
@Getter
@Setter
public class ElectionResultAck extends RequestMessage {

    @Getter
    public enum AckResult {
        Accepted(1),
        Rejected(2);
        AckResult(int value){
            this.value = value;
        }
        private int value;
    }


    // 1: 接收选举结果 2: 拒绝选举结果, 应该使用当前选举结果
    private int result;

    public ElectionResultAck(String controllerId, int epoch, int result){
        super();
        this.requestType = ServerRequestType.ElectionCompleteAck.getValue();
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.result = result;
    }

    public void buildBuffer(){
         super.toBuffer(4);
    }

    @Override
    protected void writePayload(ByteBuffer buffer){
        buffer.putInt(result);
    }

    public ByteBuffer toBuffer(){
        return super.toBuffer(4);
    }

    public static ElectionResultAck newAccept(String controllerId, int epoch){
        return new ElectionResultAck(controllerId, epoch,
                AckResult.Accepted.getValue());
    }

    public static ElectionResultAck newReject(String controllerId, int epoch){
        return new ElectionResultAck(controllerId, epoch,
                AckResult.Rejected.getValue());
    }

    public static ElectionResultAck parseFrom(RequestMessage messageBase) {
        ByteBuffer buffer = messageBase.getBuffer();
        ElectionResultAck resultAck = BeanUtil.copyProperties(messageBase, ElectionResultAck.class);
        resultAck.setResult(buffer.getInt());
        return resultAck;
    }
}
