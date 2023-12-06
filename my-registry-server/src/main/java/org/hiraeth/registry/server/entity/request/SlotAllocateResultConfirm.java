package org.hiraeth.registry.server.entity.request;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.registry.common.domain.NodeSlotInfo;
import org.hiraeth.registry.server.entity.ServerRequestType;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 12:38
 */
@Getter
@Setter
public class SlotAllocateResultConfirm extends SlotAllocateResult {

    public SlotAllocateResultConfirm(){
    }

    public SlotAllocateResultConfirm(NodeSlotInfo nodeSlotInfo){
        super(nodeSlotInfo.getSlots(), nodeSlotInfo.getSlotReplicas());
        this.requestType = ServerRequestType.AllocateSlotsConfirm.getValue();
    }

    public static SlotAllocateResultConfirm parseFrom(RequestMessage messageBase){
        SlotAllocateResult slotAllocateResult = SlotAllocateResult.parseFrom(messageBase);
        return BeanUtil.copyProperties(slotAllocateResult, SlotAllocateResultConfirm.class);
    }
}
