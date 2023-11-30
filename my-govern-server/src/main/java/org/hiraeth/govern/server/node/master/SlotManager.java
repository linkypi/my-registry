package org.hiraeth.govern.server.node.master;

import com.alibaba.fastjson.JSON;
import org.hiraeth.govern.common.util.FileUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.entity.RemoteNode;
import org.hiraeth.govern.server.node.entity.Slot;
import org.hiraeth.govern.server.node.entity.SlotRang;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 17:15
 */
public class SlotManager {

    private static final Map<Integer, Slot> slots = new ConcurrentHashMap();

    /**
     * 槽位数量， 参考Redis Cluster Hash Slots实现
     */
    private static final int SLOTS_COUNT = 16384;
    private static final String SLOTS_FILE_NAME = ".slots";
    private static final String SLOT_FILE_NAME = ".slot";

    public void initSlots(SlotRang rang){
        for (int num = rang.getStart(); num < rang.getEnd(); num++) {
            slots.put(num, new Slot(num));
        }
    }

    public Map<Integer, SlotRang> calculateSlots(List<RemoteNode> otherRemoteMasterNodes) {

        int totalMasters = otherRemoteMasterNodes.size() + 1;

        int slotsPerNode = SLOTS_COUNT / totalMasters;
        int reminds = SLOTS_COUNT - slotsPerNode * totalMasters;

        // controller 多分配多余的槽位
        int controllerSlotsCount = slotsPerNode + reminds;

        int index = 0;
        Map<Integer, SlotRang> slotsAllocation = new ConcurrentHashMap<>(totalMasters);
        for (RemoteNode remoteNode : otherRemoteMasterNodes) {
            slotsAllocation.put(remoteNode.getNodeId(), new SlotRang(index, index + slotsPerNode - 1));
            index += slotsPerNode;
        }
        int nodeId = Configuration.getInstance().getNodeId();
        slotsAllocation.put(nodeId, new SlotRang(index, controllerSlotsCount - 1));
        return slotsAllocation;
    }

    public boolean persistAllSlots(Map<Integer, SlotRang> slotsAllocation) {
        String dataDir = Configuration.getInstance().getDataDir();
        String jsonString = JSON.toJSONString(slotsAllocation);
        byte[] bytes = jsonString.getBytes();
        return FileUtil.persist(dataDir, SLOTS_FILE_NAME, bytes);
    }

    public boolean persistNodeSlots(SlotRang slots) {
        String dataDir = Configuration.getInstance().getDataDir();
        String jsonString = JSON.toJSONString(slots);
        byte[] bytes = jsonString.getBytes();
        return FileUtil.persist(dataDir, SLOT_FILE_NAME, bytes);
    }
}
