package org.hiraeth.registry.server.entity.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.server.entity.ServerRequestType;

import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 12:38
 */
@Getter
@Setter
public class SlotAllocateResultAck extends RequestMessage {
    public SlotAllocateResultAck(){
        super();
        this.requestType = ServerRequestType.AllocateSlotsAck.getValue();
    }

    public ByteBuffer toBuffer(){
        return super.toBuffer(0);
    }

    public static SlotAllocateResultAck parseFrom(RequestMessage messageBase) {
        return BeanUtil.copyProperties(messageBase, SlotAllocateResultAck.class);
    }

}
