package org.hiraeth.govern.server.node.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.NodeSlotInfo;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.*;
import org.hiraeth.govern.server.entity.request.*;
import org.hiraeth.govern.server.node.network.NIOServer;
import org.hiraeth.govern.server.node.network.ServerNetworkManager;
import org.hiraeth.govern.server.slot.SlotManager;

import java.util.Objects;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:27
 */
@Slf4j
public class ServerNode {

    private NIOServer NIOServer;

    public ServerNode() {

        ServerMessageQueue messageQueues = ServerMessageQueue.getInstance();
        messageQueues.initQueue();

        new ServerRequestHandler().start();
        this.NIOServer = new NIOServer();
//        new DetectBlockingQueueThread().start();
    }

    class DetectBlockingQueueThread extends Thread{
        public DetectBlockingQueueThread(){
        }
        @Override
        public void run() {
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            while (true){
                try {
                    Thread.sleep(10000);
                    log.info("                              ");
                    for(ServerRequestType type: ServerRequestType.values()) {
                        int countResponseMessage = messageQueue.countRequestMessage(type);
                        log.info("-->  {} queue size {}", type.name(), countResponseMessage);
                    }
                }catch (Exception ex){

                }
            }
        }
    }

    public void start() {

        Configuration configuration = Configuration.getInstance();
        RemoteServer remoteServer = new RemoteServer(configuration.getServerAddress(), configuration.isControllerCandidate());
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        remoteNodeManager.addRemoteServerNode(remoteServer);

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        // 启动线程监听
        serverNetworkManager.listenInternalPortAndWaitConnect();
        // 主动连接其他 controller.candidate.servers
        if(!serverNetworkManager.connectOtherControllerServers()){
            return;
        }
        // 投票选举 Controller Leader
        boolean isControllerCandidate = configuration.isControllerCandidate();
        if(isControllerCandidate) {
            // 等待其他controller节点都连接完成
            serverNetworkManager.waitAllTheOtherControllerConnected();

            ElectionResult electionResult = ControllerCandidate.getInstance().electController();
            NodeInfoManager nodeInfoManager = NodeInfoManager.getInstance();
            ServerRole serverRole = nodeInfoManager.updateToLeading(electionResult);

            if(serverRole == ServerRole.Controller){
                Controller controller = Controller.getInstance();
                NodeSlotInfo nodeSlotInfo = controller.allocateSlots();

                nodeInfoManager.setNodeSlotInfo(nodeSlotInfo);
            }else{
                // 接收槽位分配
                waitForControllerSlotResult();
            }
        }

        NIOServer.start();

        log.info("server has started now !!!");
    }


    /**
     * 等待 Leader 分配槽位
     */
    private void waitForControllerSlotResult() {
        try {
            log.info("wait for controller allocate slots ...");
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            while (NodeInfoManager.isRunning()) {
                if (messageQueue.countRequestMessage(ServerRequestType.AllocateSlots) > 0) {
                    acceptSlotAndReplyAck();
                    continue;
                }
                // 接收 Leader 的槽位信息的最终确认，Leader 只有收到大多数槽位分配ACK之后方才回复 Confirm
                if (messageQueue.countRequestMessage(ServerRequestType.AllocateSlotsConfirm) > 0) {
                    RequestMessage message = messageQueue.takeRequestMessage(ServerRequestType.AllocateSlotsConfirm);
                    SlotAllocateResultConfirm confirm = SlotAllocateResultConfirm.parseFrom(message);
                    if (confirm.getSlots() == null || confirm.getSlots().size() == 0) {
                        log.error("allocated slots confirm is null: {}", JSON.toJSONString(confirm));
                        NodeInfoManager.setFatal();
                        return;
                    }

                    // 初始化自身负责的槽位
                    SlotManager slotManager = SlotManager.getInstance();
                    NodeSlotInfo nodeSlotInfo = slotManager.buildCurrentNodeSlotInfo(confirm.getSlots(), confirm.getSlotReplicas());
                    slotManager.persist(nodeSlotInfo);

                    NodeInfoManager nodeInfoManager = NodeInfoManager.getInstance();
                    nodeInfoManager.setNodeSlotInfo(nodeSlotInfo);
                    break;
                }
            }
        } catch (Exception ex) {
            log.error("wait for slot allocate result occur error", ex);
            NodeInfoManager.setFatal();
        }
    }

    /**
     * 接收Leader槽位分配并回复ACK
     */
    private void acceptSlotAndReplyAck() {
        ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
        RequestMessage message = messageQueue.takeRequestMessage(ServerRequestType.AllocateSlots);
        SlotAllocateResult slotAllocateResult = SlotAllocateResult.parseFrom(message);
        if (slotAllocateResult.getSlots() == null || slotAllocateResult.getSlots().size() == 0) {
            log.error("allocated slots from controller is null: {}", JSON.toJSONString(slotAllocateResult));
            NodeInfoManager.setFatal();
            return;
        }

        // 回复ACK
        SlotAllocateResultAck resultAck = new SlotAllocateResultAck();
        resultAck.buildBuffer();

        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendRequest(slotAllocateResult.getFromNodeId(), resultAck);
        log.debug("replyed ack slot allocation");
    }
}
