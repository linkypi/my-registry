package org.hiraeth.registry.server.node.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.NodeSlotInfo;
import org.hiraeth.registry.common.domain.SlotRange;
import org.hiraeth.registry.common.domain.SlotReplica;
import org.hiraeth.registry.server.entity.request.RequestMessage;
import org.hiraeth.registry.server.entity.request.SlotAllocateResult;
import org.hiraeth.registry.server.entity.request.SlotAllocateResultAck;
import org.hiraeth.registry.server.entity.request.SlotAllocateResultConfirm;
import org.hiraeth.registry.server.node.network.ServerNetworkManager;
import org.hiraeth.registry.server.slot.SlotManager;
import org.hiraeth.registry.server.entity.RemoteServer;
import org.hiraeth.registry.server.entity.ServerRequestType;

import java.util.*;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 9:54
 */
@Slf4j
public class Controller {

    private Controller() {
    }

    public static class Singleton {
        private static final Controller instance = new Controller();
    }
    public static Controller getInstance(){
        return Singleton.instance;
    }

    public NodeSlotInfo allocateSlots() {

        log.debug("start allocate slots...");
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        List<RemoteServer> allRemoteServers = remoteNodeManager.getAllRemoteServers();

        SlotManager slotManager = SlotManager.getInstance();
        NodeSlotInfo nodeSlotInfo = slotManager.allocateSlots(otherControllerCandidates, allRemoteServers);
        syncSlots(nodeSlotInfo.getSlots(), nodeSlotInfo.getSlotReplicas());

        log.debug("persist slots success, notified other candidates, waiting for ack: {}", JSON.toJSONString(nodeSlotInfo));

        waitForSlotResultAck(nodeSlotInfo);

        return nodeSlotInfo;
    }

    private void waitForSlotResultAck(NodeSlotInfo nodeSlotInfo) {
        try {
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            Set<String> confirmSet = new HashSet<>();
            RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();

            while (NodeInfoManager.isRunning()) {
                if (messageQueue.countRequestMessage(ServerRequestType.AllocateSlotsAck) == 0) {
                    Thread.sleep(500);
                    continue;
                }

                RequestMessage message = messageQueue.takeRequestMessage(ServerRequestType.AllocateSlotsAck);
                SlotAllocateResultAck ackResult = SlotAllocateResultAck.parseFrom(message);
                confirmSet.add(ackResult.getFromNodeId());
                log.info("receive AllocateSlotsAck, confirm size {}", confirmSet.size());

                if (confirmSet.size() >= remoteNodeManager.getQuorum()) {
                    log.info("all the other candidates has confirmed the slots allocation.");

                    // 发送向各个follower发送确认结果, follower 收到确认结果后才会执行下一步操作
                    SlotAllocateResultConfirm confirmMessage= new SlotAllocateResultConfirm(nodeSlotInfo);
                    confirmMessage.buildBuffer();

                    ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
                    for (RemoteServer remoteServer : remoteNodeManager.getOtherControllerCandidates()) {
                        serverNetworkManager.sendRequest(remoteServer.getNodeId(), confirmMessage);
                        log.info("send slot allocation confirm to remote node {}.", remoteServer.getNodeId());
                    }
                    break;
                }
            }
        } catch (Exception ex) {
            log.error("wait for ack of slot allocation result occur error", ex);
            NodeInfoManager.setFatal();
        }
    }

    private void syncSlots(Map<String, List<SlotRange>> slots, Map<String, List<SlotReplica>> slotReplicas) {
        try {
            SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slots, slotReplicas);
            slotAllocateResult.buildBuffer();

            RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
            ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
            for (RemoteServer remoteServer : remoteNodeManager.getOtherControllerCandidates()) {
                serverNetworkManager.sendRequest(remoteServer.getNodeId(), slotAllocateResult);
                log.info("sync slots to remote node {} : {}.", remoteServer.getNodeId(), JSON.toJSONString(slots));
            }
        } catch (Exception ex) {
            log.error("send allocation slots to other candidates occur error: {}", JSON.toJSONString(slots), ex);
            NodeInfoManager.setFatal();
        }
    }

}
