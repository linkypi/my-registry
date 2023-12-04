package org.hiraeth.govern.server.node.core;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;
import org.hiraeth.govern.common.domain.request.*;
import org.hiraeth.govern.common.domain.response.FetchMetaDataResponse;
import org.hiraeth.govern.common.domain.response.Response;
import org.hiraeth.govern.common.domain.response.SubscribeResponse;
import org.hiraeth.govern.common.util.CommonUtil;
import org.hiraeth.govern.server.entity.*;
import org.hiraeth.govern.server.entity.request.HeartbeatForwardRequest;
import org.hiraeth.govern.server.entity.request.RegisterForwardRequest;
import org.hiraeth.govern.server.entity.response.ResponseMessage;
import org.hiraeth.govern.server.node.network.ServerNetworkManager;
import org.hiraeth.govern.server.slot.Slot;
import org.hiraeth.govern.server.node.network.ClientConnection;
import org.hiraeth.govern.server.slot.SlotManager;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 0:28
 */
@Slf4j
public class ClientRequestHandler {

    private RemoteNodeManager remoteNodeManager;
    private SlotManager slotManager;

    private ServerNetworkManager serverNetworkManager;


    public ClientRequestHandler(RemoteNodeManager remoteNodeManager, SlotManager slotManager,  ServerNetworkManager serverNetworkManager) {
        this.remoteNodeManager = remoteNodeManager;
        this.slotManager = slotManager;
        this.serverNetworkManager = serverNetworkManager;
    }

    /**
     * 回复客户端响应
     * @param connection
     */
    public void replyResponse(ClientConnection connection) {
        try {
            ClientMessageQueue messageQueue = ClientMessageQueue.getInstance();
            LinkedBlockingQueue<Message> queue = messageQueue.getMessageQueue(connection.getConnectionId());
            if (queue.isEmpty()) {
                return;
            }
            Message response = queue.peek();
            if (response == null) {
                return;
            }

            SocketChannel socketChannel = connection.getSocketChannel();
            socketChannel.write(response.getBuffer());
            log.info("reply client , request type: {}, request id: {}", response.getRequestType(), response.getRequestId());
            if (!response.getBuffer().hasRemaining()) {
                queue.poll();
            }
        } catch (Exception ex) {
            log.error("reply client occur error", ex);
        }
    }

    public Message handleRequest(ClientConnection connection, Request request) {
        if (request.getRequestType() == RequestType.FetchMetaData.getValue()) {
            FetchMetaDataRequest fetchMetaDataRequest = BeanUtil.copyProperties(request, FetchMetaDataRequest.class);
            return createMetaData(fetchMetaDataRequest);
        }
        if (request.getRequestType() == RequestType.RegisterService.getValue()) {
            return handleRegister(request);
        }
        if (request.getRequestType() == RequestType.Heartbeat.getValue()) {
            return handleHeartbeat(request);
        }
        if (request.getRequestType() == RequestType.Subscribe.getValue()) {
            return handleSubscribe(request, connection.getConnectionId());
        }

        return null;
    }

    private Response handleSubscribe(Request request, String connectionId) {
        SubscribeRequest subscribeRequest = SubscribeRequest.parseFrom(request);
        SubscribeResponse subscribeResponse = new SubscribeResponse(subscribeRequest);
        try {
            String serviceName = subscribeRequest.getServiceName();
            int routeSlot = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(routeSlot);

            List<ServiceInstanceInfo> serviceInstanceInfos = slot.subscribe(connectionId, serviceName);

            subscribeResponse.setServiceInstanceInfoAddresses(serviceInstanceInfos);
            subscribeResponse.setSuccess(true);
            log.info("client subscribe service name : {}", subscribeRequest.getServiceName());
        } catch (Exception ex) {
            subscribeResponse.setSuccess(false);
            log.error("subscribe service name occur error: {}", JSON.toJSONString(subscribeRequest), ex);
        }
        subscribeResponse.buildBuffer();
        return subscribeResponse;
    }

    private Response handleHeartbeat(Request request) {
        Response response = new Response(RequestType.Heartbeat, true);
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.parseFrom(request);

        try {
            String serviceName = heartbeatRequest.getServiceName();
            int servicePort = heartbeatRequest.getServiceInstancePort();
            String instanceIp = heartbeatRequest.getServiceInstanceIp();

            ServiceInstanceInfo serviceInstanceInfo = new ServiceInstanceInfo(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(slotNum);
            if(!slot.isReplica()) {
                slot.heartbeat(serviceInstanceInfo);
            }else{
                boolean success = forwardHeartbeatToPrimarySlotSync(slot.getNodeId(), heartbeatRequest);
                response.setSuccess(success);
            }
            log.info("heartbeat service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
        }catch (Exception ex){
            log.error("heartbeat service instance occur error: {}", JSON.toJSONString(heartbeatRequest), ex);
            response = new Response(RequestType.RegisterService, false);
        }
        response.setRequestId(request.getRequestId());
        response.buildBuffer();
        return response;
    }

    private Response handleRegister(Request request){
        Response response = new Response(RequestType.RegisterService, true);
        RegisterServiceRequest registerServiceRequest = RegisterServiceRequest.parseFrom(request);
        try {
            String serviceName = registerServiceRequest.getServiceName();
            int servicePort = registerServiceRequest.getServicePort();
            String instanceIp = registerServiceRequest.getInstanceIp();

            ServiceInstanceInfo serviceInstanceInfo = new ServiceInstanceInfo(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(slotNum);

            if(!slot.isReplica()) {
                slot.registerServiceInstance(serviceInstanceInfo);
            }else {
                // 若槽位所在机器不是当前机器则需转发到主分片所在机器
                boolean success = forwardRegisterToPrimarySlotSync(slot.getNodeId(), registerServiceRequest);
                response.setSuccess(success);
            }
            log.info("register service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
        }catch (Exception ex){
            log.error("register service instance occur error: {}", JSON.toJSONString(registerServiceRequest), ex);
            response = new Response(RequestType.RegisterService, false);
        }
        response.setRequestId(request.getRequestId());
        response.buildBuffer();
        return response;
    }

    private boolean forwardHeartbeatToPrimarySlotSync(String nodeId, HeartbeatRequest heartbeatRequest){
        String serviceName = heartbeatRequest.getServiceName();
        String serviceInstanceIp = heartbeatRequest.getServiceInstanceIp();
        int serviceInstancePort = heartbeatRequest.getServiceInstancePort();
        HeartbeatForwardRequest request = new HeartbeatForwardRequest(serviceName, serviceInstanceIp, serviceInstancePort);
        request.buildBuffer();

        log.info("forward register service to {} {}...", nodeId, serviceName);
        serverNetworkManager.sendRequest(nodeId, request);

        ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
        ResponseMessage response = messageQueue.waitForResponseMessage(ServerRequestType.HeartbeatForward);

        boolean success = response.isSuccess();
        if(success){
            log.info("forward heartbeat to {} and response success.",  nodeId);
        }else{
            log.error("forward heartbeat to {} success, but response failed: {}", nodeId, JSON.toJSONString(request));
        }

        return success;
    }

    /**
     * 同步转发请求
     *
     * @param registerServiceRequest
     */
    private boolean forwardRegisterToPrimarySlotSync(String nodeId, RegisterServiceRequest registerServiceRequest) {
        RegisterForwardRequest forwardMessage = new RegisterForwardRequest();
        forwardMessage.setInstanceIp(registerServiceRequest.getInstanceIp());
        forwardMessage.setServicePort(registerServiceRequest.getServicePort());
        forwardMessage.setServiceName(registerServiceRequest.getServiceName());
        forwardMessage.buildBuffer();

        log.info("forward register service to {} {}...", nodeId, JSON.toJSONString(registerServiceRequest));
        serverNetworkManager.sendRequest(nodeId, forwardMessage);

        ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
        ResponseMessage response = messageQueue.waitForResponseMessage(ServerRequestType.RegisterForward);

        if (response != null && response.isSuccess()) {
            log.info("forward register service to {} success: {}", nodeId, JSON.toJSONString(response));
            return true;
        }
        log.error("forward register service to {} success, but response failed: {}", nodeId, JSON.toJSONString(response));
        return false;
    }


    private FetchMetaDataResponse createMetaData(FetchMetaDataRequest request){
        NodeStatusManager nodeStatusManager = NodeStatusManager.getInstance();
        FetchMetaDataResponse fetchMetaDataResponse = new FetchMetaDataResponse();
        fetchMetaDataResponse.setRequestId(request.getRequestId());
        fetchMetaDataResponse.setSlots(nodeStatusManager.getNodeSlotInfo().getSlots());
        fetchMetaDataResponse.setServerAddresses(remoteNodeManager.getAllOnlineServerAddresses());
        fetchMetaDataResponse.buildBuffer();
        return fetchMetaDataResponse;
    }
}
