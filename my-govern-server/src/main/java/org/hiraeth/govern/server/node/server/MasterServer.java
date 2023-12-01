package org.hiraeth.govern.server.node.server;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.SlotRang;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.entity.*;
import org.hiraeth.govern.server.node.master.*;

import java.util.Map;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 17:27
 */
@Slf4j
public class MasterServer extends NodeServer {

    private MasterNetworkManager masterNetworkManager;
    /**
     * controller候选者
     */
    protected ControllerCandidate controllerCandidate;

    /**
     * 远程master节点管理组件
     */
    private final RemoteNodeManager remoteNodeManager;

    private SlotManager slotManager;

    private MasterNIOServer masterNIOServer;


    public MasterServer(){
        this.remoteNodeManager = new RemoteNodeManager();
        this.masterNetworkManager = new MasterNetworkManager(remoteNodeManager);
        this.slotManager = new SlotManager();
        this.masterNIOServer = new MasterNIOServer(slotManager);
//        new DetectBlockingQueueThread(masterNetworkManager).start();
    }

    class DetectBlockingQueueThread extends Thread{
        private MasterNetworkManager masterNetworkManager;
        public DetectBlockingQueueThread(MasterNetworkManager masterNetworkManager){
            this.masterNetworkManager = masterNetworkManager;
        }
        @Override
        public void run() {
            while (true){
                try {
                    Thread.sleep(10000);
                    log.info("                              ");
                    for(MessageType type: MessageType.values()) {
                        int countResponseMessage = masterNetworkManager.countResponseMessage(type);
                        log.info("-->  {} queue size {}", type.name(), countResponseMessage);
                    }
                }catch (Exception ex){

                }
            }
        }
    }

    @Override
    public void start() {

        Configuration configuration = Configuration.getInstance();
        RemoteNode remoteNode = new RemoteNode(configuration.getNodeId(),
                configuration.getNodeType(), configuration.isControllerCandidate());
        remoteNodeManager.addRemoteMasterNode(remoteNode);

        // 启动线程监听 id 比当前节点id 大的master节点的连接请求
        masterNetworkManager.waitGreaterIdMasterNodeConnect();
        // 主动连接 id 比当前节点id 较小的master节点
        if(!masterNetworkManager.connectLowerIdMasterNodes()){
            return;
        }
        // 等待其他master节点都连接完成
        masterNetworkManager.waitAllTheOtherNodesConnected();

        // 投票选举 Controller
        boolean isControllerCandidate = configuration.isControllerCandidate();
        if(isControllerCandidate) {
            this.controllerCandidate = new ControllerCandidate(masterNetworkManager, remoteNodeManager);
            ElectionResult electionResult = controllerCandidate.voteForControllerElection();

            // update current node status
            NodeStatusManager instance = NodeStatusManager.getInstance();
            instance.updateStatus(electionResult);

            ElectionStage.setStatus(ElectionStage.ELStage.LEADING);
            int leaderId = electionResult.getControllerId();
            MasterRole masterRole = MasterRole.Candidate;
            int currentNodeId = Configuration.getInstance().getNodeId();
            if (currentNodeId == leaderId) {
                masterRole = MasterRole.Controller;
                log.info("leader start on current node, epoch {} !!!", electionResult.getEpoch());
            }
            electionResult.setMasterRole(masterRole);

            if(masterRole == MasterRole.Controller){
                Controller controller = new Controller(remoteNodeManager, masterNetworkManager);
                controller.allocateSlots();
            }else{
                // 接收槽位分配
                waitForControllerSlotResult();
            }
        }

        // 监听slave节点发起的请求
        masterNetworkManager.waitSlaveNodeConnect();

        masterNIOServer.start();
    }

    private void waitForControllerSlotResult() {
        try {
            log.info("wait for controller allocate slots ...");
            while (NodeStatusManager.isRunning()) {
                if (masterNetworkManager.countResponseMessage(MessageType.AllocateSlots) > 0) {
                    acceptSlotAndReplyAck();
                    continue;
                }
                if (masterNetworkManager.countResponseMessage(MessageType.AllocateSlotsConfirm) > 0) {
                    MessageBase message = masterNetworkManager.takeResponseMessage(MessageType.AllocateSlotsConfirm);
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
        MessageBase message = masterNetworkManager.takeResponseMessage(MessageType.AllocateSlots);
        SlotAllocateResult slotAllocateResult = SlotAllocateResult.parseFrom(message);
        if (slotAllocateResult.getSlots() == null || slotAllocateResult.getSlots().size() == 0) {
            log.error("allocated slots from controller is null: {}", JSON.toJSONString(slotAllocateResult));
            NodeStatusManager.setFatal();
            return true;
        }

        // 回复ACK
        SlotAllocateResultAck resultAck = new SlotAllocateResultAck();
        masterNetworkManager.sendRequest(slotAllocateResult.getFromNodeId(), resultAck.toMessage());
        log.debug("replyed ack slot allocation");
        return true;
    }

    private void persistSlots(SlotAllocateResult slotAllocateResult) {
        Map<Integer, SlotRang> slots = slotAllocateResult.getSlots();
        int nodeId = Configuration.getInstance().getNodeId();
        slotManager.persistAllSlots(slots);
        slotManager.persistNodeSlots(slots.get(nodeId));

        NodeStatusManager.getInstance().setSlots(slotAllocateResult.getSlots());

        log.debug("persist slots success: {}", JSON.toJSONString(slots));
    }
}
