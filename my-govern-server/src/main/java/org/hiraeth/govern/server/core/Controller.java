package org.hiraeth.govern.server.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.SlotRang;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.*;

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

    public Map<String, SlotRang> allocateSlots() {

        log.debug("start allocate slots...");
        List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
        Map<String, SlotRang> slotRangMap = slotManager.calculateSlots(otherControllerCandidates);

        // 持久化槽位分配结果
        boolean success = slotManager.persistAllSlots(slotRangMap);
        if (!success) {
            NodeStatusManager.setFatal();
            return null;
        }

        syncSlots(slotRangMap);

        // 初始化自身负责的槽位
        String nodeId = Configuration.getInstance().getNodeId();
        SlotRang slotRang = slotRangMap.get(nodeId);
        slotManager.initSlots(slotRang);

        // 持久化自身负责的槽位
        success = slotManager.persistNodeSlots(slotRang);
        if (!success) {
            NodeStatusManager.setFatal();
            return null;
        }

        log.debug("persist slots success, notified other candidates, waiting for ack: {}", JSON.toJSONString(slotRangMap));
        waitForSlotResultAck(slotRangMap);
        return slotRangMap;
    }

    private void waitForSlotResultAck(Map<String, SlotRang> slotRangMap) {
        try {
            Set<String> confirmSet = new HashSet<>();
            while (NodeStatusManager.isRunning()) {
                if (serverNetworkManager.countResponseMessage(MessageType.AllocateSlotsAck) == 0) {
                    Thread.sleep(500);
                    continue;
                }

                MessageBase message = serverNetworkManager.takeResponseMessage(MessageType.AllocateSlotsAck);
                SlotAllocateResultAck ackResult = SlotAllocateResultAck.parseFrom(message);
                confirmSet.add(ackResult.getFromNodeId());
                log.info("receive AllocateSlotsAck, confirm size {}", confirmSet.size());

                if (confirmSet.size() >= remoteNodeManager.getQuorum()) {
                    log.info("all the other candidates has confirmed the slots allocation.");

                    // 发送向各个follower发送确认结果, follower 收到确认结果后才会执行下一步操作
                    SlotAllocateResultConfirm confirmMessage= new SlotAllocateResultConfirm(slotRangMap);
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

    private void syncSlots(Map<String, SlotRang> slotRangMap) {
        try {
            SlotAllocateResult slotAllocateResult = new SlotAllocateResult(slotRangMap);
            for (RemoteServer remoteServer : remoteNodeManager.getOtherControllerCandidates()) {
                serverNetworkManager.sendRequest(remoteServer.getNodeId(), slotAllocateResult.toMessage());
                log.info("sync slots to remote node {} : {}.", remoteServer.getNodeId(), JSON.toJSONString(slotRangMap));
            }
        } catch (Exception ex) {
            log.error("send allocation slots to other candidates occur error: {}", JSON.toJSONString(slotRangMap), ex);
            NodeStatusManager.setFatal();
        }
    }
}
