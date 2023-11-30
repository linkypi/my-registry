package org.hiraeth.govern.server.node.master;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.util.FileUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.entity.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 9:54
 */
@Slf4j
public class Controller {

    /**
     * 槽位数量， 参考Redis Cluster Hash Slots实现
     */
    private static final int SLOTS_COUNT = 16384;
    private static final String SLOTS_ALLOCATION_FILE_NAME = "slot_allocation";

    private RemoteNodeManager remoteNodeManager;
    private MasterNetworkManager masterNetworkManager;

    private Map<Integer, SlotRang> slotsAllocation;

    public Controller(RemoteNodeManager remoteNodeManager, MasterNetworkManager masterNetworkManager) {
        this.remoteNodeManager = remoteNodeManager;
        this.masterNetworkManager = masterNetworkManager;
    }

    public void allocateSlots() {

        calculateSlots();

        // 持久化槽位分配结果
        boolean success = persistSlotsAllocation(slotsAllocation);
        if (!success) {
            NodeStatusManager.setFatal();
            return;
        }

        notifySlotsAllocation();

        log.debug("persist slots success, notified other candidates, waiting for ack ...");
        waitForSlotAllocateResultAck();
    }

    private void waitForSlotAllocateResultAck() {
        try {
            Set<Integer> confirmSet = new HashSet<>();
            while (NodeStatusManager.isRunning()) {
                if (masterNetworkManager.countResponseMessage(MessageType.AllocateSlotsAck) == 0) {
                    Thread.sleep(500);
                    continue;
                }
                MessageBase message = masterNetworkManager.takeResponseMessage(MessageType.AllocateSlotsAck);
                SlotAllocateResultAck ackResult = SlotAllocateResultAck.parseFrom(message);
                confirmSet.add(ackResult.getFromNodeId());
                if (confirmSet.size() >= remoteNodeManager.getQuorum()) {
                    log.info("all the other candidates has confirmed the slots allocation.");
                    break;
                }
            }
        } catch (Exception ex) {
            log.error("wait for ack of slot allocation result occur error", ex);
            NodeStatusManager.setFatal();
        }
    }

    private void notifySlotsAllocation() {
        try {
            SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slotsAllocation);
            for (RemoteNode remoteNode : remoteNodeManager.getOtherControllerCandidates()) {
                masterNetworkManager.sendRequest(remoteNode.getNodeId(), slotAllocateResult.toMessage());
            }
        } catch (Exception ex) {
            log.error("send allocation slots to other candidates occur error: {}", JSON.toJSONString(slotsAllocation), ex);
            NodeStatusManager.setFatal();
        }
    }

    private void calculateSlots() {
        List<RemoteNode> otherRemoteMasterNodes = remoteNodeManager.getOtherControllerCandidates();

        int totalMasters = otherRemoteMasterNodes.size() + 1;

        int slotsPerNode = SLOTS_COUNT / totalMasters;
        int reminds = SLOTS_COUNT - slotsPerNode * totalMasters;

        // controller 多分配多余的槽位
        int controllerSlotsCount = slotsPerNode + reminds;

        int index = 0;
        slotsAllocation = new ConcurrentHashMap<>(totalMasters);
        for (RemoteNode remoteNode : otherRemoteMasterNodes) {
            slotsAllocation.put(remoteNode.getNodeId(), new SlotRang(index, index + slotsPerNode - 1));
            index += slotsPerNode;
        }
        int nodeId = Configuration.getInstance().getNodeId();
        slotsAllocation.put(nodeId, new SlotRang(index, controllerSlotsCount - 1));
    }

    public static boolean persistSlotsAllocation(Map<Integer, SlotRang> slotsAllocation) {
        String dataDir = Configuration.getInstance().getDataDir();
        String jsonString = JSON.toJSONString(slotsAllocation);
        byte[] bytes = jsonString.getBytes();
        return FileUtil.persist(dataDir, SLOTS_ALLOCATION_FILE_NAME, bytes);
    }
}
