package org.hiraeth.govern.server.entity;

import cn.hutool.core.bean.BeanUtil;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.SlotRang;

import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 12:38
 */
@Getter
@Setter
public class SlotAllocateResultConfirm extends SlotAllocateResult{

    public SlotAllocateResultConfirm(Map<String, SlotRang> slots){
        super(slots);
        this.clusterMessageType = ClusterMessageType.AllocateSlotsConfirm;
    }
    public static SlotAllocateResultConfirm parseFrom(ClusterBaseMessage messageBase){
        SlotAllocateResult slotAllocateResult = SlotAllocateResult.parseFrom(messageBase);
        return BeanUtil.copyProperties(slotAllocateResult, SlotAllocateResultConfirm.class);
    }
}
