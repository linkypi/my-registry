package org.hiraeth.govern.server.ha;

import cn.hutool.core.date.LocalDateTimeUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.NodeSlotInfo;
import org.hiraeth.govern.common.domain.ServerAddress;
import org.hiraeth.govern.common.domain.SlotRange;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.RemoteServer;
import org.hiraeth.govern.server.entity.ServerRequestType;
import org.hiraeth.govern.server.entity.ServerRole;
import org.hiraeth.govern.server.entity.request.BeAliveAskRequest;
import org.hiraeth.govern.server.entity.request.ElectionResult;
import org.hiraeth.govern.server.entity.request.RequestMessage;
import org.hiraeth.govern.server.entity.response.ResponseMessage;
import org.hiraeth.govern.server.node.core.*;
import org.hiraeth.govern.server.node.network.ServerNetworkManager;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/5 9:09
 */
@Getter
@Setter
@Slf4j
public class HighAvailabilityManager {


    private HighAvailabilityManager() {
        new Worker().start();
        new ReceiveWorker().start();
    }

    public static class Singleton {
        private static final HighAvailabilityManager instance = new HighAvailabilityManager();
    }

    private static final long WAIT_FRO_TIMEOUT = 5000;
    private static final long DETECT_RETRY_TIMES = 3;
    public static HighAvailabilityManager getInstance() {
        return HighAvailabilityManager.Singleton.instance;
    }

    private static final BlockingQueue<String> disconnectedQueue = new LinkedBlockingQueue<>();

    private static RemoteNodeManager remoteNodeManager;
    private static ServerNetworkManager serverNetworkManager;

    /**
     * 集群节点断开连接后的处理
     * 1. 若是controller节点端口连接，则需更新集群状态，禁止客户端请求，并重新发起选举，同步新分配的槽位数据
     * 2. 若是其他普通节点断开，则需均衡负载的槽位数据
     * @param remoteNodeId
     */
    public void handleDisconnectedException(String remoteNodeId){
        disconnectedQueue.add(remoteNodeId);
    }

    static class ReceiveWorker extends Thread {
        @Override
        public void run() {
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
            Configuration configuration = Configuration.getInstance();
            while (true){

                RequestMessage message = messageQueue.takeRequestMessage(ServerRequestType.BeAliveAsk);
                BeAliveAskRequest beAliveAskRequest = BeAliveAskRequest.parseFrom(message);
                BeAliveAskRequest.Status status = BeAliveAskRequest.Status.Down;

                // 首先检查是否已存在连接， 没有存在连接则发起连接
                boolean connected = remoteNodeManager.isConnected(beAliveAskRequest.getRemoteNodeId());
                if(connected){
                    status = BeAliveAskRequest.Status.Alive;
                }else{
                    // 没有存在连接 则重新连接
                    ServerAddress serverAddress = remoteNodeManager.getServerAddress(beAliveAskRequest.getRemoteNodeId());
                    boolean connectSuccess = serverNetworkManager.connectControllerNode(serverAddress);
                    if(connectSuccess){
                        status = BeAliveAskRequest.Status.Alive;
                    }
                }
                replyBeAliveAsk(status, message.getFromNodeId());
            }
        }

        private void replyBeAliveAsk(BeAliveAskRequest.Status status, String remoteNodeId) {
            ResponseMessage responseMessage = new ResponseMessage();
            responseMessage.setCode(status.getValue());
            responseMessage.buildBuffer();
            serverNetworkManager.sendRequest(remoteNodeId, responseMessage);
        }
    }

    static class Worker extends Thread{
        @Override
        public void run() {
            Configuration configuration = Configuration.getInstance();

            while (true){
                try {
                    String disconnectedNodeId = disconnectedQueue.take();

                    // 向其他节点确认 remoteNodeId 是否已经断开连接, 因为有可能仅仅只是当前节点与 remoteNodeId 无法连接
                    // 即单节点的决策无法决定集群决策
                    boolean isDown = detectAliveFromOtherCandidates(disconnectedNodeId);
                    if(!isDown) {
                        log.debug("current node is disconnected from {}, but most other " +
                                "candidate controller connect normally.", disconnectedNodeId);
                        continue;
                    }

                    NodeInfoManager nodeInfoManager = NodeInfoManager.getInstance();
                    String controllerId = nodeInfoManager.getControllerId();

                    // 若当前节点为 controller 节点, 则 remoteNode 必定不是 controller 节点
                    if(disconnectedNodeId.equals(controllerId)){
                        // leader 已宕机, 更新集群状态, 重新发起选举
                        ElectionStage.setStatus(ElectionStage.ELStage.ELECTING);
                        NodeInfoManager.getInstance().setElectingStage();

                        ControllerCandidate controllerCandidate = ControllerCandidate.getInstance();
                        ElectionResult electionResult = controllerCandidate.electController();
                        ServerRole serverRole = nodeInfoManager.updateToLeading(electionResult);

                        if(serverRole == ServerRole.Controller){
                            // 重新分配槽位
                            Controller controller = Controller.getInstance();
                            NodeSlotInfo newNodeSlotInfo = controller.allocateSlots();

                            // 获取到宕机节点原有存储的槽位数据
                            NodeSlotInfo oldNodeSlotInfo = nodeInfoManager.getNodeSlotInfo();
                            List<SlotRange> ranges = oldNodeSlotInfo.getSlots().get(disconnectedNodeId);

                            // 宕机节点的副本数据


                            // 更新节点槽位信息
                            //nodeInfoManager.setNodeSlotInfo(nodeSlotInfo);
                        }else{
                            // 接收槽位分配
//                            waitForControllerSlotResult();
                        }


                    }else{
                        // leader 节点服务正常, 普通服务节点宕机, 均衡分配槽位数据
                    }

                }catch (Exception ex){
                    log.error("high available manager worker occur error", ex);
                }
            }
        }

        private static boolean detectAliveFromOtherCandidates(String disconnectedNodeId) throws InterruptedException {

            BeAliveAskRequest.Status status = detectAlive(disconnectedNodeId);

            // 若超时返回 null 则重试
            int retryTime = 0;
            while (status == null && retryTime < DETECT_RETRY_TIMES){
                status = detectAlive(disconnectedNodeId);
                retryTime ++;
            }

            return status == null || BeAliveAskRequest.Status.Down == status;
        }

        private static BeAliveAskRequest.Status detectAlive(String disconnectedNodeId) throws InterruptedException {
            BeAliveAskRequest beAliveAskRequest = new BeAliveAskRequest(disconnectedNodeId);
            ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();

            // 向其他节点发送远程节点是否存活的请求
            List<RemoteServer> otherControllerCandidates = remoteNodeManager.getOtherControllerCandidates();
            for (RemoteServer remoteServer: otherControllerCandidates){
                serverNetworkManager.sendRequest(remoteServer.getNodeId(), beAliveAskRequest);
            }

            Set<String> downSet = new HashSet<>(otherControllerCandidates.size());
            Set<String> aliveSet = new HashSet<>(otherControllerCandidates.size());
            LocalDateTime startTime = LocalDateTimeUtil.now();

            while (true){
                Thread.sleep(200);

                // 超出 WAIT_FRO_TIMEOUT 时间仍获得响应则返回null
                LocalDateTime now = LocalDateTimeUtil.now();
                Duration duration = LocalDateTimeUtil.between(startTime, now).minusMillis(WAIT_FRO_TIMEOUT);
                if (!duration.isNegative()) {
                    return null;
                }

                int count = messageQueue.countResponseMessage(ServerRequestType.BeAliveAsk);
                if(count == 0 ){
                    continue;
                }
                ResponseMessage response = messageQueue.takeResponseMessage(ServerRequestType.BeAliveAsk);
                if(response != null && response.isSuccess()){
                    int quorum = otherControllerCandidates.size() / 2 + 1;
                    if(response.getCode() == BeAliveAskRequest.Status.Down.getValue()){
                        log.debug("the disconnected remote node {} is down by {} detection.", disconnectedNodeId, response.getFromNodeId());
                        downSet.add(response.getFromNodeId());
                        if(downSet.size() >= quorum){
                            return BeAliveAskRequest.Status.Down;
                        }
                    }else{
                        log.debug("the disconnected remote node {} is alive by {} detection.", disconnectedNodeId, response.getFromNodeId());
                        aliveSet.add(response.getFromNodeId());
                        if(aliveSet.size() >= quorum){
                            return BeAliveAskRequest.Status.Alive;
                        }
                    }
                }
            }
        }
    }
}
