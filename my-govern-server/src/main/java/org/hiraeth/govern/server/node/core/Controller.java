package org.hiraeth.govern.server.node.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.NodeSlotInfo;
import org.hiraeth.govern.common.domain.SlotRange;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.*;
import org.hiraeth.govern.server.node.network.ServerNetworkManager;
import org.hiraeth.govern.server.slot.SlotManager;

import java.util.*;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 9:54
 */
@Slf4j
public class Controller {

    private RemoteNodeManager remoteNodeManager;
    private ServerNetworkManager serverNetworkManager;
    private SlotManager slotManager;

    public Controller(RemoteNodeManager remoteNodeManager, ServerNetworkManager serverNetworkManager) {
        this.remoteNodeManager = remoteNodeManager;
        this.serverNetworkManager = serverNetworkManager;
        this.slotManager = new SlotManager();
    }

    public NodeSlotInfo allocateSlots() {

        log.debug("start allocate slots...");
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        Map<String, SlotRange> slots = slotManager.executeSlotsAllocation(otherControllerCandidates);

        // 分配slots副本
        Map<String,Map<String, List<SlotRange>>> slotReplicas = slotManager.executeSlotReplicasAllocation(slots, remoteNodeManager.getAllRemoteServers());

        NodeSlotInfo nodeSlotInfo = slotManager.buildCurrentNodeSlotInfo(slots, slotReplicas);

        // 持久化槽位分配结果
        boolean success = slotManager.persistNodeSlotsInfo(nodeSlotInfo);
        if (!success) {
            NodeStatusManager.setFatal();
            return null;
        }

        syncSlots(slots, slotReplicas);

        // 初始化自身负责的槽位
        slotManager.initSlots(nodeSlotInfo.getSlotRange());

        // 持久化自身负责的槽位
        success = slotManager.persistNodeSlots(nodeSlotInfo.getSlotRange());
        if (!success) {
            NodeStatusManager.setFatal();
            return null;
        }

        log.debug("persist slots success, notified other candidates, waiting for ack: {}", JSON.toJSONString(slots));
        waitForSlotResultAck(nodeSlotInfo);
        return nodeSlotInfo;
    }

    /**
     * 执行slots副本分配
     */


    private void waitForSlotResultAck(NodeSlotInfo nodeSlotInfo) {
        try {
            Set<String> confirmSet = new HashSet<>();
            while (NodeStatusManager.isRunning()) {
                if (serverNetworkManager.countResponseMessage(ClusterMessageType.AllocateSlotsAck) == 0) {
                    Thread.sleep(500);
                    continue;
                }

                ClusterBaseMessage message = serverNetworkManager.takeResponseMessage(ClusterMessageType.AllocateSlotsAck);
                SlotAllocateResultAck ackResult = SlotAllocateResultAck.parseFrom(message);
                confirmSet.add(ackResult.getFromNodeId());
                log.info("receive AllocateSlotsAck, confirm size {}", confirmSet.size());

                if (confirmSet.size() >= remoteNodeManager.getQuorum()) {
                    log.info("all the other candidates has confirmed the slots allocation.");

                    // 发送向各个follower发送确认结果, follower 收到确认结果后才会执行下一步操作
                    SlotAllocateResultConfirm confirmMessage= new SlotAllocateResultConfirm(nodeSlotInfo);
                    for (RemoteServer remoteServer : remoteNodeManager.getOtherControllerCandidates()) {
                        serverNetworkManager.sendRequest(remoteServer.getNodeId(), confirmMessage.toMessage());
                        log.info("send slot allocation confirm to remote node {}.", remoteServer.getNodeId());
                    }
                    break;
                }
            }
        } catch (Exception ex) {
            log.error("wait for ack of slot allocation result occur error", ex);
            NodeStatusManager.setFatal();
        }
    }

    private void syncSlots(Map<String, SlotRange> slots, Map<String,Map<String, List<SlotRange>>> slotReplicas) {
        try {
            SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slots, slotReplicas);
            for (RemoteServer remoteServer : remoteNodeManager.getOtherControllerCandidates()) {
                serverNetworkManager.sendRequest(remoteServer.getNodeId(), slotAllocateResult.toMessage());
                log.info("sync slots to remote node {} : {}.", remoteServer.getNodeId(), JSON.toJSONString(slots));
            }
        } catch (Exception ex) {
            log.error("send allocation slots to other candidates occur error: {}", JSON.toJSONString(slots), ex);
            NodeStatusManager.setFatal();
        }
    }

}
