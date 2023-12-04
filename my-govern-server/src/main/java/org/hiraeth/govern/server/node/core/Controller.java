package org.hiraeth.govern.server.node.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.NodeSlotInfo;
import org.hiraeth.govern.common.domain.SlotRange;
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

    public Controller(RemoteNodeManager remoteNodeManager, ServerNetworkManager serverNetworkManager, SlotManager slotManager) {
        this.remoteNodeManager = remoteNodeManager;
        this.serverNetworkManager = serverNetworkManager;
        this.slotManager = slotManager;
    }

    public NodeSlotInfo allocateSlots() {

        log.debug("start allocate slots...");
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        List<RemoteServer> allRemoteServers = remoteNodeManager.getAllRemoteServers();

        NodeSlotInfo nodeSlotInfo = slotManager.allocateSlots(otherControllerCandidates, allRemoteServers);
        syncSlots(nodeSlotInfo.getSlots(), nodeSlotInfo.getSlotReplicas());

        log.debug("persist slots success, notified other candidates, waiting for ack: {}", JSON.toJSONString(nodeSlotInfo));

        waitForSlotResultAck(nodeSlotInfo);

        return nodeSlotInfo;
    }

    /**
     * 执行slots副本分配
     */


    private void waitForSlotResultAck(NodeSlotInfo nodeSlotInfo) {
        try {
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            Set<String> confirmSet = new HashSet<>();
            while (NodeStatusManager.isRunning()) {
                if (messageQueue.countElectingMessage(ClusterMessageType.AllocateSlotsAck) == 0) {
                    Thread.sleep(500);
                    continue;
                }

                ClusterBaseMessage message = messageQueue.takeElectingMessage(ClusterMessageType.AllocateSlotsAck);
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
