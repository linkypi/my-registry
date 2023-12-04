package org.hiraeth.govern.server.node.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.ServiceInstanceInfo;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.server.entity.*;
import org.hiraeth.govern.server.node.network.ServerNetworkManager;
import org.hiraeth.govern.server.slot.Slot;
import org.hiraeth.govern.server.slot.SlotManager;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/4 14:26
 */
@Slf4j
public class ServerRequestHandler extends Thread{

    private SlotManager slotManager;
    private ServerNetworkManager serverNetworkManager;

    private static final long WAIT_TIME_INTERVAL = 100;

    ServerRequestHandler( SlotManager slotManager, ServerNetworkManager serverNetworkManager) {
        this.slotManager = slotManager;
        this.serverNetworkManager = serverNetworkManager;
    }

    @Override
    public void run() {
        ServerMessageQueue messageQueues = ServerMessageQueue.getInstance();
        try {
            log.info("start server request handler.");
            while (NodeStatusManager.isRunning()) {

                int registerForwardCount = messageQueues.countLeadingMessage(ClusterMessageType.RegisterForward);
                if (registerForwardCount > 0) {
                    handleRegisterForward(messageQueues);
                }
                int heartbeatForwardCount = messageQueues.countLeadingMessage(ClusterMessageType.HeartbeatForward);
                if (heartbeatForwardCount > 0) {
                    handleHeartbeatForward(messageQueues);
                }

                Thread.sleep(WAIT_TIME_INTERVAL);
            }
        }catch (Exception ex){
            log.error("server request handler occur error", ex);
        }
    }

    private void handleHeartbeatForward(ServerMessageQueue messageQueues) {
        ClusterBaseMessage message = messageQueues.takeElectingMessage(ClusterMessageType.HeartbeatForward);
        HeartbeatForwardRequest heartbeatForwardRequest = HeartbeatForwardRequest.parseFrom(message);

        ClusterBaseMessage response = new ClusterBaseMessage();
        response.setMessageId(heartbeatForwardRequest.getMessageId());

        try {
            String serviceName = heartbeatForwardRequest.getServiceName();
            int servicePort = heartbeatForwardRequest.getServiceInstancePort();
            String instanceIp = heartbeatForwardRequest.getServiceInstanceIp();

            ServiceInstanceInfo serviceInstanceInfo = new ServiceInstanceInfo(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(slotNum);

            if (!slot.isReplica()) {
                slot.heartbeat(serviceInstanceInfo);
                response.setSuccess(true);
                log.info("heartbeat service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
            } else {
                // 消息已经转发，若到此仍未找到主分片则说明集群节点之间的槽位信息没有同步
                log.error("heartbeat service instance from forward node {} failed: {}, because the the primary slot not found.",
                        message.getFromNodeId(), JSON.toJSONString(serviceInstanceInfo));
            }

        } catch (Exception ex) {
            log.error("heartbeat service instance occur error: {}", JSON.toJSONString(heartbeatForwardRequest), ex);
        }

        ClusterMessage clusterMessage = response.toMessage();
        serverNetworkManager.sendRequest(message.getFromNodeId(), clusterMessage);
    }
    private void handleRegisterForward(ServerMessageQueue messageQueues) {
        ClusterBaseMessage message = messageQueues.takeElectingMessage(ClusterMessageType.RegisterForward);
        RegisterForwardRequest registerForwardRequest = RegisterForwardRequest.parseFrom(message);

        RegisterForwardResponse registerForwardResponse = new RegisterForwardResponse(true);
        registerForwardResponse.setMessageId(registerForwardRequest.getMessageId());

        try {
            String serviceName = registerForwardRequest.getServiceName();
            int servicePort = registerForwardRequest.getServicePort();
            String instanceIp = registerForwardRequest.getInstanceIp();

            ServiceInstanceInfo serviceInstanceInfo = new ServiceInstanceInfo(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(slotNum);

            if (!slot.isReplica()) {
                slot.registerServiceInstance(serviceInstanceInfo);
                log.info("register service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
            } else {
                // 消息已经转发，若到此仍未找到主分片则说明集群节点之间的槽位信息没有同步
                registerForwardResponse.setSuccess(false);
                log.error("register service instance from forward node {} failed: {}, because the the primary slot not found.",
                        message.getFromNodeId(), JSON.toJSONString(serviceInstanceInfo));
            }

        } catch (Exception ex) {
            log.error("register service instance occur error: {}", JSON.toJSONString(registerForwardRequest), ex);
            registerForwardResponse.setSuccess(false);
        }

        ClusterMessage clusterMessage = registerForwardResponse.toMessage();
        serverNetworkManager.sendRequest(message.getFromNodeId(), clusterMessage);
    }

}
