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

    public ClientRequestHandler(RemoteNodeManager remoteNodeManager, SlotManager slotManager) {
        this.remoteNodeManager = remoteNodeManager;
        this.slotManager = slotManager;
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
        if (request.getRequestType() == RequestType.FetchMetaData) {
            FetchMetaDataRequest fetchMetaDataRequest = BeanUtil.copyProperties(request, FetchMetaDataRequest.class);
            return createMetaData(fetchMetaDataRequest);
        }
        if (request.getRequestType() == RequestType.RegisterService) {
            return saveServiceInstance(request);
        }
        if (request.getRequestType() == RequestType.Heartbeat) {
            return handleHeartbeat(request);
        }
        if (request.getRequestType() == RequestType.Subscribe) {
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
            slot.heartbeat(serviceInstanceInfo);
            log.info("heartbeat service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
        }catch (Exception ex){
            log.error("heartbeat service instance occur error: {}", JSON.toJSONString(heartbeatRequest), ex);
            response = new Response(RequestType.RegisterService, false);
        }
        response.setRequestId(request.getRequestId());
        response.buildBuffer();
        return response;
    }

    private Response saveServiceInstance(Request request) {
        Response response = new Response(RequestType.RegisterService, true);
        RegisterServiceRequest registerServiceRequest = RegisterServiceRequest.parseFrom(request);
        try {
            String serviceName = registerServiceRequest.getServiceName();
            int servicePort = registerServiceRequest.getServicePort();
            String instanceIp = registerServiceRequest.getInstanceIp();

            ServiceInstanceInfo serviceInstanceInfo = new ServiceInstanceInfo(serviceName, instanceIp, servicePort);

            int slotNum = CommonUtil.routeSlot(serviceName);
            Slot slot = slotManager.getSlot(slotNum);
            slot.registerServiceInstance(serviceInstanceInfo);
            log.info("register service instance success: {}", JSON.toJSONString(serviceInstanceInfo));
        }catch (Exception ex){
            log.error("register service instance occur error: {}", JSON.toJSONString(registerServiceRequest), ex);
            response = new Response(RequestType.RegisterService, false);
        }
        response.setRequestId(request.getRequestId());
        response.buildBuffer();
        return response;
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
