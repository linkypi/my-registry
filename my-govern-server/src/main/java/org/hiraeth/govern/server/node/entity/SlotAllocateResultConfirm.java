package org.hiraeth.govern.server.node.entity;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.SlotRang;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 12:38
 */
@Getter
@Setter
public class SlotAllocateResultConfirm extends SlotAllocateResult{

    public SlotAllocateResultConfirm(Map<Integer, SlotRang> slots){
        super(slots);
        this.messageType = MessageType.AllocateSlotsConfirm;
    }
    public static SlotAllocateResultConfirm parseFrom(MessageBase messageBase){
        SlotAllocateResult slotAllocateResult = SlotAllocateResult.parseFrom(messageBase);
        return BeanUtil.copyProperties(slotAllocateResult, SlotAllocateResultConfirm.class);
    }
}
