package org.hiraeth.govern.server.node.master;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.entity.*;

import java.util.*;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 9:54
 */
@Slf4j
public class Controller {


    private RemoteNodeManager remoteNodeManager;
    private MasterNetworkManager masterNetworkManager;
    private SlotManager slotManager;

    public Controller(RemoteNodeManager remoteNodeManager, MasterNetworkManager masterNetworkManager) {
        this.remoteNodeManager = remoteNodeManager;
        this.masterNetworkManager = masterNetworkManager;
        this.slotManager = new SlotManager();
    }

    public void allocateSlots() {

        List<RemoteNode> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        Map<Integer, SlotRang> slotRangMap = slotManager.calculateSlots(otherControllerCandidates);

        // 持久化槽位分配结果
        boolean success = slotManager.persistAllSlots(slotRangMap);
        if (!success) {
            NodeStatusManager.setFatal();
            return;
        }

        syncSlots(slotRangMap);

        // 初始化自身负责的槽位
        int nodeId = Configuration.getInstance().getNodeId();
        SlotRang slotRang = slotRangMap.get(nodeId);
        slotManager.initSlots(slotRang);

        // 持久化自身负责的槽位
        success = slotManager.persistNodeSlots(slotRang);
        if (!success) {
            NodeStatusManager.setFatal();
            return;
        }

        log.debug("persist slots success, notified other candidates, waiting for ack: {}", JSON.toJSONString(slotRangMap));
        waitForSlotResultAck();
    }

    private void waitForSlotResultAck() {
        try {
            Set<Integer> confirmSet = new HashSet<>();
            while (NodeStatusManager.isRunning()) {
//                if (masterNetworkManager.countResponseMessage(MessageType.AllocateSlotsAck) == 0) {
//                    Thread.sleep(500);
//                    continue;
//                }
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

    private void syncSlots(Map<Integer, SlotRang> slotRangMap) {
        try {
            SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slotRangMap);
            for (RemoteNode remoteNode : remoteNodeManager.getOtherControllerCandidates()) {
                masterNetworkManager.sendRequest(remoteNode.getNodeId(), slotAllocateResult.toMessage());
                log.info("sync slots to remote node {} : {}.", remoteNode.getNodeId(), JSON.toJSONString(slotRangMap));
            }
        } catch (Exception ex) {
            log.error("send allocation slots to other candidates occur error: {}", JSON.toJSONString(slotRangMap), ex);
            NodeStatusManager.setFatal();
        }
    }
}
