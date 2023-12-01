package org.hiraeth.govern.server.entity;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 12:38
 */
@Getter
@Setter
public class SlotAllocateResultAck extends MessageBase{
    public SlotAllocateResultAck(){
        super();
        this.messageType = MessageType.AllocateSlotsAck;
    }
    public Message toMessage(){
        ByteBuffer buffer = toBuffer();
        return new Message(messageType, buffer.array());
    }
    public ByteBuffer toBuffer(){
        return super.convertToBuffer(0);
    }

    public static SlotAllocateResultAck parseFrom(MessageBase messageBase) {
        return BeanUtil.copyProperties(messageBase, SlotAllocateResultAck.class);
    }

}
