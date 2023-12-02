package org.hiraeth.govern.server.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.SlotRang;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.core.*;
import org.hiraeth.govern.server.entity.*;
import org.hiraeth.govern.server.core.NodeStatusManager;
import org.hiraeth.govern.server.network.NIOServer;
import org.hiraeth.govern.server.network.ServerNetworkManager;

import java.util.Map;
import java.util.Objects;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:27
 */
@Slf4j
public class ServerInstance {

    private ServerNetworkManager serverNetworkManager;
    /**
     * controller候选者
     */
    protected ControllerCandidate controllerCandidate;

    /**
     * 远程controller节点管理组件
     */
    private final RemoteNodeManager remoteNodeManager;

    private SlotManager slotManager;

    private org.hiraeth.govern.server.network.NIOServer NIOServer;


    public ServerInstance(){
        this.remoteNodeManager = new RemoteNodeManager();
        this.serverNetworkManager = new ServerNetworkManager(remoteNodeManager);
        this.slotManager = new SlotManager();
        this.NIOServer = new NIOServer(remoteNodeManager);
//        new DetectBlockingQueueThread(NetworkManager).start();
    }

    class DetectBlockingQueueThread extends Thread{
        private ServerNetworkManager serverNetworkManager;
        public DetectBlockingQueueThread(ServerNetworkManager serverNetworkManager){
            this.serverNetworkManager = serverNetworkManager;
        }
        @Override
        public void run() {
            while (true){
                try {
                    Thread.sleep(10000);
                    log.info("                              ");
                    for(MessageType type: MessageType.values()) {
                        int countResponseMessage = serverNetworkManager.countResponseMessage(type);
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
        remoteNodeManager.addRemoteServerNode(remoteServer);

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

            this.controllerCandidate = new ControllerCandidate(serverNetworkManager, remoteNodeManager);
            ElectionResult electionResult = controllerCandidate.voteForControllerElection();

            ElectionStage.setStatus(ElectionStage.ELStage.LEADING);
            String leaderId = electionResult.getControllerId();
            ServerRole serverRole = ServerRole.Candidate;
            String currentNodeId = Configuration.getInstance().getNodeId();
            if (Objects.equals(currentNodeId, leaderId)) {
                serverRole = ServerRole.Controller;
                log.info("leader start on current node, epoch {} !!!", electionResult.getEpoch());
            }
            electionResult.setServerRole(serverRole);

            // update current node status
            NodeStatusManager nodeStatusManager = NodeStatusManager.getInstance();
            nodeStatusManager.updateStatus(electionResult, ElectionStage.ELStage.LEADING);

            if(serverRole == ServerRole.Controller){
                Controller controller = new Controller(remoteNodeManager, serverNetworkManager);
                Map<String, SlotRang> integerSlotRangMap = controller.allocateSlots();
                nodeStatusManager.setSlots(integerSlotRangMap);
            }else{
                // 接收槽位分配
                waitForControllerSlotResult();
            }
        }

        NIOServer.start();

        log.info("server has started now !!!");
    }

    private void waitForControllerSlotResult() {
        try {
            log.info("wait for controller allocate slots ...");
            while (NodeStatusManager.isRunning()) {
                if (serverNetworkManager.countResponseMessage(MessageType.AllocateSlots) > 0) {
                    acceptSlotAndReplyAck();
                    continue;
                }
                if (serverNetworkManager.countResponseMessage(MessageType.AllocateSlotsConfirm) > 0) {
                    MessageBase message = serverNetworkManager.takeResponseMessage(MessageType.AllocateSlotsConfirm);
                    SlotAllocateResultConfirm confirm = SlotAllocateResultConfirm.parseFrom(message);
                    if (confirm.getSlots() == null || confirm.getSlots().size() == 0) {
                        log.error("allocated slots confirm is null: {}", JSON.toJSONString(confirm));
                        NodeStatusManager.setFatal();
                        return;
                    }
                    persistSlots(confirm);
                    break;
                }
            }
        } catch (Exception ex) {
            log.error("wait for slot allocate result occur error", ex);
            NodeStatusManager.setFatal();
        }
    }

    private boolean acceptSlotAndReplyAck() {
        MessageBase message = serverNetworkManager.takeResponseMessage(MessageType.AllocateSlots);
        SlotAllocateResult slotAllocateResult = SlotAllocateResult.parseFrom(message);
        if (slotAllocateResult.getSlots() == null || slotAllocateResult.getSlots().size() == 0) {
            log.error("allocated slots from controller is null: {}", JSON.toJSONString(slotAllocateResult));
            NodeStatusManager.setFatal();
            return true;
        }

        // 回复ACK
        SlotAllocateResultAck resultAck = new SlotAllocateResultAck();
        serverNetworkManager.sendRequest(slotAllocateResult.getFromNodeId(), resultAck.toMessage());
        log.debug("replyed ack slot allocation");
        return true;
    }

    private void persistSlots(SlotAllocateResult slotAllocateResult) {
        Map<String, SlotRang> slots = slotAllocateResult.getSlots();
        String nodeId = Configuration.getInstance().getNodeId();
        slotManager.persistAllSlots(slots);
        slotManager.persistNodeSlots(slots.get(nodeId));

        NodeStatusManager.getInstance().setSlots(slotAllocateResult.getSlots());

        log.debug("persist slots success: {}", JSON.toJSONString(slots));
    }
}
