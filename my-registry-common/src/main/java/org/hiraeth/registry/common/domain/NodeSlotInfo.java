package org.hiraeth.registry.common.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * @author: lynch
 * @description: 每个节点的槽位信息
 * @date: 2023/12/3 20:03
 */
@Getter
@Setter
@AllArgsConstructor
public class NodeSlotInfo {
    // 节点id
    private String nodeId;

    // 节点负责的槽位, 使用集合存储是为了方便节点故障时接管其他槽位
    private List<SlotRange> slotRanges;

    // 槽位分片，即一份完整数据会拆分到几个节点存储
    private Map<String, List<SlotRange>> slots;
    // 当前节点存放的其他节点槽位副本信息
    private Map<String, List<SlotReplica>> slotReplicas;
}
