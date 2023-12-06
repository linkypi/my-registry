package org.hiraeth.registry.server.slot;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.NodeSlotInfo;
import org.hiraeth.registry.common.domain.ServerAddress;
import org.hiraeth.registry.common.domain.SlotReplica;
import org.hiraeth.registry.common.util.FileUtil;
import org.hiraeth.registry.server.config.Configuration;
import org.hiraeth.registry.server.entity.RemoteServer;
import org.hiraeth.registry.common.domain.SlotRange;
import org.hiraeth.registry.server.node.core.NodeInfoManager;
import org.hiraeth.registry.server.slot.registry.SlotHeartbeatThread;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.hiraeth.registry.common.constant.Constant.SLOTS_COUNT;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 17:15
 */
@Setter
@Getter
@Slf4j
public class SlotManager {

    public static class Singleton {
        private static final SlotManager instance = new SlotManager();
    }
    public static SlotManager getInstance(){
        return Singleton.instance;
    }

    // 槽位实际数据，包括了副本槽位
    private static final Map<Integer, Slot> slots = new ConcurrentHashMap();

    // 存储槽位副本所在机器地址
    private static final Map<SlotRange, ServerAddress> slotReplicaAddresses = new ConcurrentHashMap();

    /**
     * 槽位数量， 参考Redis Cluster Hash Slots实现
     */
    private static final String SLOTS_FILE_NAME = ".slots";
    private static final String SLOTS_REPLICA_FILE_NAME = ".slots_replicas";
    private static final String SLOT_FILE_NAME = ".slot";


    private SlotManager(){
        new SlotHeartbeatThread(this).start();
    }

    public Slot getSlot(int slotNum){
        return slots.get(slotNum);
    }

    public List<Slot> getSlots(){
        return new ArrayList<>(slots.values());
    }

    private void initSlotsAndReplicas(List<SlotRange> slotRanges, Map<String, List<SlotReplica>> slotReplicasMap) {

        // init
        Configuration configuration = Configuration.getInstance();
        String curNodeId = configuration.getNodeId();
        for (SlotRange item: slotRanges) {
            for (int num = item.getStart(); num <= item.getEnd(); num++) {
                slots.put(num, new Slot(num, false, curNodeId));
            }
        }

        // 初始化槽位副本所在机器地址 以及槽位副本
        List<SlotReplica> replicas = slotReplicasMap.get(curNodeId);
        for (SlotReplica replica : replicas) {
            String nodeId = replica.getNodeId();

            ServerAddress serverAddr = configuration.getControllerServers().get(nodeId);
            List<SlotRange> ranges = replica.getSlotRanges();
            for (SlotRange range: slotRanges) {
                slotReplicaAddresses.put(range, serverAddr);
                for (int num = range.getStart(); num <= range.getEnd(); num++) {
                    slots.put(num, new Slot(num, true, nodeId));
                }
            }
        }
    }

    public NodeSlotInfo allocateSlots(List<RemoteServer> otherControllerCandidates, List<RemoteServer> allRemoteServers) {
        // 分配slots
        Map<String, List<SlotRange>> slots = executeSlotsAllocation(otherControllerCandidates);
        log.debug("allocate slots: {}", JSON.toJSONString(slots));

        // 分配slots副本
        Map<String, List<SlotReplica>> slotReplicas = executeSlotReplicasAllocation(slots, allRemoteServers);
        log.debug("allocate slots replicas: {}", JSON.toJSONString(slotReplicas));

        NodeSlotInfo nodeSlotInfo = buildCurrentNodeSlotInfo(slots, slotReplicas);
        // 持久化槽位分配结果
        boolean success = persist(nodeSlotInfo);
        if (!success) {
            return null;
        }
        return nodeSlotInfo;
    }

    public boolean persist(NodeSlotInfo nodeSlotInfo){
        // 持久化槽位分配结果
        boolean success = persistNodeSlotsInfo(nodeSlotInfo);
        if (!success) {
            NodeInfoManager.setFatal();
            return false;
        }
        // 持久化自身负责的槽位
        success = persistNodeSlots(nodeSlotInfo.getSlotRanges());
        if (!success) {
            NodeInfoManager.setFatal();
            return false;
        }

        // 持久化完成后初始化内存
        initSlotsAndReplicas(nodeSlotInfo.getSlotRanges(), nodeSlotInfo.getSlotReplicas());

        return true;
    }

    private Map<String, List<SlotRange>> executeSlotsAllocation(List<RemoteServer> otherRemoteServerNodes) {

        int totalServers = otherRemoteServerNodes.size() + 1;

        int slotsPerNode = SLOTS_COUNT / totalServers;
        int reminds = SLOTS_COUNT - slotsPerNode * totalServers;

        // controller 多分配多余的槽位
        int controllerSlotsCount = slotsPerNode + reminds;

        int index = 0;
        Map<String, List<SlotRange>> slotsAllocation = new ConcurrentHashMap<>(totalServers);
        for (RemoteServer remoteServer : otherRemoteServerNodes) {
            List<SlotRange> ranges = slotsAllocation.get(remoteServer.getNodeId());
            if (ranges == null) {
                ranges = new ArrayList<>();
            }
            ranges.add(new SlotRange(index, index + slotsPerNode - 1));
            slotsAllocation.put(remoteServer.getNodeId(), ranges);
            index += slotsPerNode;
        }
        String nodeId = Configuration.getInstance().getNodeId();

        List<SlotRange> ranges = new ArrayList<>();
        ranges.add(new SlotRange(index, index + controllerSlotsCount - 1));
        slotsAllocation.put(nodeId, ranges);
        return slotsAllocation;
    }

    /**
     * 分配副本
     * @param slots
     * @param allRemoteServers
     * @return
     */
    private Map<String, List<SlotReplica>> executeSlotReplicasAllocation(Map<String, List<SlotRange>> slots, List<RemoteServer> allRemoteServers) {

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
        Map<String, List<SlotReplica>> slotsReplicasMap = new ConcurrentHashMap<>();

        for (String nodeId : slots.keySet()) {
            String replicaNodeId = null;
            int replicas = 0;

            Map<String, List<SlotReplica>> tempReplicas = new ConcurrentHashMap<>();
            tempReplicas.put(nodeId, new ArrayList<>());

            // 寻找其他节点的副本，保存到当前 nodeId 节点，每个节点的副本总数为 replicas
            while (replicas < numberOfReplicas) {
                int index = random.nextInt(allNodeIds.size());
                replicaNodeId = allNodeIds.get(index);
                List<SlotReplica> stringListMap = tempReplicas.get(nodeId);

                // 同一节点不分配副本
                if(nodeId.equals(replicaNodeId)){
                    continue;
                }
                // 已分配的节点不再分配
                String finalReplicaNodeId = replicaNodeId;
                boolean hasAllocate = stringListMap.stream()
                        .anyMatch(a -> Objects.equals(a.getNodeId(), finalReplicaNodeId));
                if (!hasAllocate) {
                    List<SlotReplica> slotReplicas = tempReplicas.get(nodeId);
                    List<SlotRange> ranges = slots.get(replicaNodeId);

                    List<SlotRange> slotRanges = BeanUtil.copyToList(ranges, SlotRange.class);
                    SlotReplica slotReplica = new SlotReplica(replicaNodeId, slotRanges);
                    slotReplicas.add(slotReplica);
                    tempReplicas.put(nodeId, slotReplicas);
                    replicas++;
                }
            }
            List<SlotReplica> stringListMap = tempReplicas.get(nodeId);
            slotsReplicasMap.put(nodeId, stringListMap);
        }

        return slotsReplicasMap;
    }

    public NodeSlotInfo buildCurrentNodeSlotInfo(Map<String, List<SlotRange>> slots, Map<String, List<SlotReplica>> slotReplicas){
        String nodeId = Configuration.getInstance().getNodeId();
        List<SlotRange> slotRanges = slots.get(nodeId);
        List<SlotRange> ranges = BeanUtil.copyToList(slotRanges, SlotRange.class);
        return new NodeSlotInfo(nodeId, ranges, slots, slotReplicas);
    }

    private boolean persistNodeSlotsInfo(NodeSlotInfo nodeSlotInfo) {
        return persisSlots(nodeSlotInfo, SLOTS_FILE_NAME);
    }

    private boolean persisSlots(Object slotsAllocation, String fileName){
        String dataDir = Configuration.getInstance().getDataDir();
        String jsonString = JSON.toJSONString(slotsAllocation);
        byte[] bytes = jsonString.getBytes();
        return FileUtil.persist(dataDir, fileName, bytes);
    }

    private boolean persistNodeSlots(List<SlotRange> slots) {
        String dataDir = Configuration.getInstance().getDataDir();
        String jsonString = JSON.toJSONString(slots);
        byte[] bytes = jsonString.getBytes();
        return FileUtil.persist(dataDir, SLOT_FILE_NAME, bytes);
    }


}
