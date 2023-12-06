package org.hiraeth.registry.server.node.core;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.Message;
import org.hiraeth.registry.common.domain.RequestType;
import org.hiraeth.registry.common.domain.ServiceInstanceInfo;
import org.hiraeth.registry.common.domain.SlotReplica;
import org.hiraeth.registry.common.domain.request.*;
import org.hiraeth.registry.common.domain.response.FetchMetaDataResponse;
import org.hiraeth.registry.common.domain.response.Response;
import org.hiraeth.registry.common.domain.response.ResponseStatusCode;
import org.hiraeth.registry.common.domain.response.SubscribeResponse;
import org.hiraeth.registry.common.util.CommonUtil;
import org.hiraeth.registry.server.config.Configuration;
import org.hiraeth.registry.server.entity.request.HeartbeatForwardRequest;
import org.hiraeth.registry.server.entity.request.RegisterForwardRequest;
import org.hiraeth.registry.server.entity.response.ResponseMessage;
import org.hiraeth.registry.server.node.network.ServerNetworkManager;
import org.hiraeth.registry.server.slot.Slot;
import org.hiraeth.registry.server.node.network.ClientConnection;
import org.hiraeth.registry.server.slot.SlotManager;
import org.hiraeth.registry.server.entity.ServerRequestType;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/12/1 0:28
 */
@Slf4j
public class ClientRequestHandler {

    private ClientRequestHandler() {
    }

    public static class Singleton {
        private static final ClientRequestHandler instance = new ClientRequestHandler();
    }
    public static ClientRequestHandler getInstance(){
        return Singleton.instance;
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

            SlotManager slotManager = SlotManager.getInstance();
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
            SlotManager slotManager = SlotManager.getInstance();
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
            SlotManager slotManager = SlotManager.getInstance();
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

    private void checkSlotLocation(Slot slot, ClientConnection connection, Request request){

        // 若主分片在当前机器则直接可以处理
        Configuration configuration = Configuration.getInstance();
        String currentNodeId = configuration.getNodeId();
        if(currentNodeId.equals(slot.getNodeId())){

            // TODO: 2023/12/5  主分片节点正常, 此时当前节点可以处理读写请求, 处理完成后返回

            return;
        }

        // 若主分片所在节点已经与当前节点断开连接 则直接查询其对应的副本分片返回
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        boolean connected = remoteNodeManager.isConnected(slot.getNodeId());
        if(connected){

            // TODO: 2023/12/5  主分片节点正常, 此时主分片节点可以处理读写请求
            // TODO: 2023/12/5  当前节点需转发请求到主分片节点进行处理, 处理完成后返回当前节点, 而后由当前节点返回客户端

            return;
        }

        // 到此, 主分片已经不可用, 即主分片节点已无法提供服务, 此时只能处理读请求, 无法处理写请求
        // 紧接着只能从主分片所在副本节点中去查询到相关数据
        NodeInfoManager nodeInfoManager = NodeInfoManager.getInstance();
        Map<String, List<SlotReplica>> slotReplicas = nodeInfoManager.getNodeSlotInfo().getSlotReplicas();
        if(slotReplicas.containsKey(slot.getNodeId())){
            List<SlotReplica> replicas = slotReplicas.get(slot.getNodeId());
            // 检查该副本是否在当前机器, 若在则直接处理后返回, 仅针对读请求, 写请求需集群完成宕机处理后方可处理
            boolean exist = replicas.stream().anyMatch(a -> a.getNodeId().equals(currentNodeId));
            if(exist){

                // TODO: 2023/12/5  从本机获取数据后返回
                return;
            }

            // TODO: 2023/12/5  否则转发到其他机器获取数据
            return;
        }

        // 否则直接返回当前集群不可用
        ClientMessageQueue messageQueue = ClientMessageQueue.getInstance();
        String connectionId = connection.getConnectionId();
        if(nodeInfoManager.getStage() == ElectionStage.ELStage.ELECTING){
            Response response = new Response(ResponseStatusCode.Electing.getCode());
            response.setRequestId(request.getRequestId());
            response.setRequestType(request.getRequestType());
            messageQueue.addMessage(connectionId, response);
        }
    }

    private boolean forwardHeartbeatToPrimarySlotSync(String nodeId, HeartbeatRequest heartbeatRequest){
        String serviceName = heartbeatRequest.getServiceName();
        String serviceInstanceIp = heartbeatRequest.getServiceInstanceIp();
        int serviceInstancePort = heartbeatRequest.getServiceInstancePort();
        HeartbeatForwardRequest request = new HeartbeatForwardRequest(serviceName, serviceInstanceIp, serviceInstancePort);
        request.buildBuffer();

        log.info("forward register service to {} {}...", nodeId, serviceName);
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
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
        ServerNetworkManager serverNetworkManager = ServerNetworkManager.getInstance();
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
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        NodeInfoManager nodeInfoManager = NodeInfoManager.getInstance();
        FetchMetaDataResponse fetchMetaDataResponse = new FetchMetaDataResponse();
        fetchMetaDataResponse.setRequestId(request.getRequestId());
        fetchMetaDataResponse.setSlots(nodeInfoManager.getNodeSlotInfo().getSlots());
        fetchMetaDataResponse.setServerAddresses(remoteNodeManager.getAllOnlineServerAddresses());
        fetchMetaDataResponse.buildBuffer();
        return fetchMetaDataResponse;
    }
}
