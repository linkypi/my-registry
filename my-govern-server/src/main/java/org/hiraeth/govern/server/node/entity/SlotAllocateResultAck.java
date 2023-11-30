package org.hiraeth.govern.server.node.entity;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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
        return super.newBuffer(0);
    }

    public static SlotAllocateResultAck parseFrom(MessageBase messageBase) {
        return BeanUtil.copyProperties(messageBase, SlotAllocateResultAck.class);
    }

}
