package org.hiraeth.govern.server.node.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

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
public class ElectionResultAck {

    @Getter
    public enum AckResult {
        Accepted(1),
        Rejected(2);
        AckResult(int value){
            this.value = value;
        }
        private int value;
    }

    private int controllerId;
    private int epoch;
    private int fromNodeId;
    // 1: 接收选举结果 2: 拒绝选举结果, 应该使用当前选举结果
    private int result;

    public ElectionResultAck(int controllerId, int epoch, int fromNodeId, int result){
        this.controllerId = controllerId;
        this.epoch = epoch;
        this.fromNodeId = fromNodeId;
        this.result = result;
    }

    public ByteBuffer toBuffer(){
        ByteBuffer buffer = ByteBuffer.allocate(24);
        buffer.putInt(RequestType.ElectionCompleteAck.getValue());
        buffer.putInt(controllerId);
        buffer.putInt(epoch);
        buffer.putInt(fromNodeId);
        buffer.putInt(result);
        return buffer;
    }

    public static ElectionResultAck newAccept(int controllerId, int epoch, int fromNodeId){
        return new ElectionResultAck(controllerId, epoch,
                fromNodeId, AckResult.Accepted.getValue());
    }

    public static ElectionResultAck newReject(int controllerId, int epoch, int fromNodeId){
        return new ElectionResultAck(controllerId, epoch,
                fromNodeId, AckResult.Rejected.getValue());
    }

    public static ElectionResultAck parseFrom(ByteBuffer buffer) {
        int controllerId = buffer.getInt();
        int epoch = buffer.getInt();
        int fromNodeId = buffer.getInt();
        int result = buffer.getInt();
        return new ElectionResultAck(controllerId, epoch, fromNodeId, result);
    }
}
