package org.hiraeth.govern.server.node.entity;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 13:58
 */
@Getter
@Setter
public class MessageBase {
    private MessageType messageType;
    private int controllerId;
    private int epoch;
    private long timestamp;
    private int fromNodeId;

    private ByteBuffer buffer;

    static class A extends MessageBase{
        private int a;
        private int b;

        public ByteBuffer toBuffer(){
            return super.newBuffer(8);
        }

        static void parseFrom(MessageType messageType, ByteBuffer buffer){
            MessageBase messageBase = MessageBase.parseFrom(buffer);

        }

        @Override
        protected void writePayload(ByteBuffer buffer){
            buffer.putInt(a);
            buffer.putInt(b);
        }
    }

    protected void readPayload(ByteBuffer buffer){

    }

    protected void writePayload(ByteBuffer buffer){
    }

    protected ByteBuffer newBuffer(int payloadLength) {
        ByteBuffer buffer = ByteBuffer.allocate(24 + payloadLength);
        buffer.putInt(messageType.getValue());
        buffer.putInt(controllerId);
        buffer.putInt(epoch);
        buffer.putLong(System.currentTimeMillis());
        buffer.putInt(fromNodeId);
        writePayload(buffer);
        return buffer;
    }

    protected static MessageBase parseFrom(ByteBuffer buffer) {
        int requestType = buffer.getInt();
        MessageType msgType = MessageType.of(requestType);

        MessageBase messageBase = new MessageBase();
        messageBase.setMessageType(msgType);
        messageBase.controllerId = buffer.getInt();
        messageBase.epoch = buffer.getInt();
        messageBase.timestamp = buffer.getLong();
        messageBase.fromNodeId = buffer.getInt();
        return messageBase;
    }

}
