package org.hiraeth.govern.server.slot;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import org.hiraeth.govern.common.domain.NodeSlotInfo;
import org.hiraeth.govern.common.util.FileUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.RemoteServer;
import org.hiraeth.govern.common.domain.SlotRange;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.hiraeth.govern.common.constant.Constant.SLOTS_COUNT;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 17:15
 */
@Setter
@Getter
public class SlotManager {

    private static final Map<Integer, Slot> slots = new ConcurrentHashMap();

    /**
     * 槽位数量， 参考Redis Cluster Hash Slots实现
     */
    private static final String SLOTS_FILE_NAME = ".slots";
    private static final String SLOTS_REPLICA_FILE_NAME = ".slots_replicas";
    private static final String SLOT_FILE_NAME = ".slot";

    public Slot getSlot(int num){
        return slots.get(num);
    }

    public void initSlots(SlotRange rang){
        for (int num = rang.getStart(); num < rang.getEnd(); num++) {
            slots.put(num, new Slot(num));
        }
    }

    public Map<String, SlotRange> executeSlotsAllocation(List<RemoteServer> otherRemoteServerNodes) {

        int totalServers = otherRemoteServerNodes.size() + 1;

        int slotsPerNode = SLOTS_COUNT / totalServers;
        int reminds = SLOTS_COUNT - slotsPerNode * totalServers;

        // controller 多分配多余的槽位
        int controllerSlotsCount = slotsPerNode + reminds;

        int index = 0;
        Map<String, SlotRange> slotsAllocation = new ConcurrentHashMap<>(totalServers);
        for (RemoteServer remoteServer : otherRemoteServerNodes) {
            slotsAllocation.put(remoteServer.getNodeId(), new SlotRange(index, index + slotsPerNode - 1));
            index += slotsPerNode;
        }
        String nodeId = Configuration.getInstance().getNodeId();
        slotsAllocation.put(nodeId, new SlotRange(index, index + controllerSlotsCount - 1));
        return slotsAllocation;
    }

    /**
     * 分配副本
     * @param slots
     * @param allRemoteServers
     * @return
     */
    public Map<String,Map<String, List<SlotRange>>> executeSlotReplicasAllocation(Map<String, SlotRange> slots, List<RemoteServer> allRemoteServers) {

        // 获取所有节点的nodeId
        List<String> allNodeIds = new ArrayList<>();
        Configuration configuration = Configuration.getInstance();
        int numberOfReplicas = configuration.getNumberOfReplicas();

        // 副本数最多为 N-1 份， 即除当前节点以外的集群节点每个节点最多1份
        if (numberOfReplicas >= allRemoteServers.size()) {
            numberOfReplicas = allRemoteServers.size() - 1;
        }

        allRemoteServers.forEach(a -> {
            allNodeIds.add(a.getNodeId());
        });

        // 执行副本分配
        Random random = new Random();

        // 存放的是每份副本所在机器列表信息， 由于副本信息对应的是节点信息故使用节点id作为key
        Map<String, Map<String, List<SlotRange>>> slotsReplicas = new ConcurrentHashMap<>();

        for (String nodeId : slots.keySet()) {
            SlotRange slotRange = slots.get(nodeId);

            String replicaNodeId = null;
            int replicas = 0;

            Map<String, Map<String, List<SlotRange>>> tempReplicas = new ConcurrentHashMap<>();
            tempReplicas.put(nodeId, new HashMap<>());

            while (replicas < numberOfReplicas) {
                int index = random.nextInt(allNodeIds.size());
                replicaNodeId = allNodeIds.get(index);
                Map<String, List<SlotRange>> stringListMap = tempReplicas.get(nodeId);
                if (!nodeId.equals(replicaNodeId) && !stringListMap.containsKey(replicaNodeId)) {
                    Map<String, List<SlotRange>> slotRanges = tempReplicas.get(nodeId);
                    if (slotRanges == null) {
                        Map<String, List<SlotRange>> listMap = new HashMap<>();
                        listMap.put(replicaNodeId, new ArrayList<>());
                        tempReplicas.put(nodeId, listMap);
                    }
                    List<SlotRange> ranges = tempReplicas.get(nodeId).get(replicaNodeId);
                    if (ranges == null) {
                        ranges = new ArrayList<>();
                    }
                    ranges.add(slotRange);
                    tempReplicas.get(nodeId).put(replicaNodeId, ranges);
                    replicas++;
                }
            }
            Map<String, List<SlotRange>> stringListMap = tempReplicas.get(nodeId);
            slotsReplicas.put(nodeId, stringListMap);
        }

        return slotsReplicas;
    }

    public NodeSlotInfo buildCurrentNodeSlotInfo(Map<String, SlotRange> slots, Map<String,Map<String, List<SlotRange>>> slotReplicas){
        String nodeId = Configuration.getInstance().getNodeId();
        SlotRange slotRange = slots.get(nodeId);
        return new NodeSlotInfo(nodeId, slotRange, slots, slotReplicas);
    }

    public boolean persistNodeSlotsInfo(NodeSlotInfo nodeSlotInfo) {
        return persisSlots(nodeSlotInfo, SLOTS_FILE_NAME);
    }

    private boolean persisSlots(Object slotsAllocation, String fileName){
        String dataDir = Configuration.getInstance().getDataDir();
        String jsonString = JSON.toJSONString(slotsAllocation);
        byte[] bytes = jsonString.getBytes();
        return FileUtil.persist(dataDir, fileName, bytes);
    }

    public boolean persistNodeSlots(SlotRange slots) {
        String dataDir = Configuration.getInstance().getDataDir();
        String jsonString = JSON.toJSONString(slots);
        byte[] bytes = jsonString.getBytes();
        return FileUtil.persist(dataDir, SLOT_FILE_NAME, bytes);
    }


}
