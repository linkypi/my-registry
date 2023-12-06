package org.hiraeth.registry.server.node.core;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.ServiceInstanceInfo;
import org.hiraeth.registry.common.util.CommonUtil;
import org.hiraeth.registry.server.entity.request.HeartbeatForwardRequest;
import org.hiraeth.registry.server.entity.request.RegisterForwardRequest;
import org.hiraeth.registry.server.entity.request.RequestMessage;
import org.hiraeth.registry.server.entity.response.ResponseMessage;
import org.hiraeth.registry.server.node.network.ServerNetworkManager;
import org.hiraeth.registry.server.slot.Slot;
import org.hiraeth.registry.server.slot.SlotManager;
import org.hiraeth.registry.server.entity.ServerRequestType;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/4 14:26
 */
@Slf4j
public class ServerRequestHandler extends Thread{

    private static final long WAIT_TIME_INTERVAL = 100;

    ServerRequestHandler() {
    }

    @Override
    public void run() {
        ServerMessageQueue messageQueues = ServerMessageQueue.getInstance();
        try {
            log.info("start server request handler.");
            while (NodeInfoManager.isRunning()) {

                int registerForwardCount = messageQueues.countRequestMessage(ServerRequestType.RegisterForward);
                if (registerForwardCount > 0) {
                    handleRegisterForward(messageQueues);
                }
                int heartbeatForwardCount = messageQueues.countRequestMessage(ServerRequestType.HeartbeatForward);
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
        RequestMessage message = messageQueues.takeRequestMessage(ServerRequestType.HeartbeatForward);
        HeartbeatForwardRequest heartbeatForwardRequest = HeartbeatForwardRequest.parseFrom(message);

        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setRequestType(message.getRequestType());
        responseMessage.setRequestId(heartbeatForwardRequest.getRequestId());

        try {
            String serviceName = heartbeatForwardRequest.getServiceName();
            int servicePort = heartbeatForwardRequest.getServiceInstancePort();
            String instanceIp = heartbeatForwardRequest.getServiceInstanceIp();

            ServiceInstanceInfo serviceInstanceInfo = new ServiceInstanceInfo(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            SlotManager slotManager = SlotManager.getInstance();
            Slot slot = slotManager.getSlot(slotNum);

            if (!slot.isReplica()) {
                slot.heartbeat(serviceInstanceInfo);
                responseMessage.setSuccess(true);
                log.info("heartbeat service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
            } else {
                // 消息已经转发，若到此仍未找到主分片则说明集群节点之间的槽位信息没有同步
                log.error("heartbeat service instance from forward node {} failed: {}, because the the primary slot not found.",
                        message.getFromNodeId(), JSON.toJSONString(serviceInstanceInfo));
            }

        } catch (Exception ex) {
            log.error("heartbeat service instance occur error: {}", JSON.toJSONString(heartbeatForwardRequest), ex);
        }

        responseMessage.buildBuffer();
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendRequest(message.getFromNodeId(), responseMessage);
    }
    private void handleRegisterForward(ServerMessageQueue messageQueues) {
        RequestMessage message = messageQueues.takeRequestMessage(ServerRequestType.RegisterForward);
        RegisterForwardRequest registerForwardRequest = RegisterForwardRequest.parseFrom(message);

        ResponseMessage responseMessage = new ResponseMessage();
        responseMessage.setRequestType(message.getRequestType());
        responseMessage.setRequestId(registerForwardRequest.getRequestId());

        try {
            String serviceName = registerForwardRequest.getServiceName();
            int servicePort = registerForwardRequest.getServicePort();
            String instanceIp = registerForwardRequest.getInstanceIp();

            ServiceInstanceInfo serviceInstanceInfo = new ServiceInstanceInfo(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            SlotManager slotManager = SlotManager.getInstance();
            Slot slot = slotManager.getSlot(slotNum);

            if (!slot.isReplica()) {
                slot.registerServiceInstance(serviceInstanceInfo);
                responseMessage.setSuccess(true);
                log.info("register service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
            } else {
                // 消息已经转发，若到此仍未找到主分片则说明集群节点之间的槽位信息没有同步
                log.error("register service instance from forward node {} failed: {}, because the the primary slot not found.",
                        message.getFromNodeId(), JSON.toJSONString(serviceInstanceInfo));
            }
        } catch (Exception ex) {
            log.error("register service instance occur error: {}", JSON.toJSONString(registerForwardRequest), ex);
        }

        responseMessage.buildBuffer();
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
        serverNetworkManager.sendRequest(message.getFromNodeId(), responseMessage);
    }

}
