package com.hiraeth.govern.client;

import com.alibaba.fastjson.JSON;
import com.hiraeth.govern.client.config.Configuration;
import com.hiraeth.govern.client.network.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;
import org.hiraeth.govern.common.domain.request.FetchMetaDataRequest;
import org.hiraeth.govern.common.domain.request.ServiceChangedRequest;
import org.hiraeth.govern.common.domain.request.RegisterServiceRequest;
import org.hiraeth.govern.common.domain.request.SubscribeRequest;
import org.hiraeth.govern.common.domain.response.FetchMetaDataResponse;
import org.hiraeth.govern.common.domain.response.Response;
import org.hiraeth.govern.common.domain.response.SubscribeResponse;
import org.hiraeth.govern.common.util.CollectionUtil;
import org.hiraeth.govern.common.util.CommonUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static org.hiraeth.govern.common.constant.Constant.REQUEST_WAIT_SLEEP_INTERVAL;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 18:36
 */
@Getter
@Setter
@Slf4j
public class ServiceInstance {

    // io 多路复用组件
    private Selector selector;
    // 客户端与服务端的selectionKey
    private SelectionKey selectionKey;

    private ConcurrentHashMap<Long, Response> responses = new ConcurrentHashMap<>();

    private ServerConnectionManager serverConnectionManager;
    private Map<String, SlotRange> slotsMap;
    private List<ServerAddress> serverAddresses;
    // 当前连接的服务器
    private ServerAddress server;
    private CountDownLatch fetchMataDataLatch = new CountDownLatch(1);

    private ServerConnection controllerConnection;
    private ServerConnection routeServerConnection;
    private static Map<String, List<ServiceInstanceInfo>> cacheServiceRegistry = new ConcurrentHashMap<>();

    private static final Random random = new Random();

    public ServiceInstance() {
        try {
            selector = Selector.open();
            serverConnectionManager = new ServerConnectionManager();

            new IOThread(this, serverConnectionManager, responses).start();
        } catch (IOException ex) {
            log.error("start nio selector error", ex);
        } catch (Exception ex) {
            log.error("start nio selector error", ex);
        }
    }

    /**
     * 随机选择一个controller候选节点, 与其建立长连接
     * 发送请求到controller候选节点获取slots数据
     */
    public void init() throws IOException, InterruptedException {
        connectController();
        registerCurrentInstance();
    }

    private void registerCurrentInstance() throws IOException, InterruptedException {
        ServerAddress serverAddress = routeServer();
        checkConnection(serverAddress);
        register();
    }

    private void checkConnection(ServerAddress serverAddress) throws IOException, InterruptedException {
        if(!serverConnectionManager.hasConnect(serverAddress.toString())){
            routeServerConnection = connectServer(serverAddress);
        }
    }

    private void connectController() throws IOException, InterruptedException {
        server = chooseControllerCandidate();
        controllerConnection = connectServer(server);
        fetchMetaDataFromServer(controllerConnection.getConnectionId());
    }

    public void reconnect(ServerConnection connection) throws IOException, InterruptedException {

        if (controllerConnection == null || connection.getAddress().equals(controllerConnection.getAddress())) {
            connectController();
        }
        if (routeServerConnection == null || connection.getAddress().equals(routeServerConnection.getAddress())) {
            registerCurrentInstance();
        }
    }

    /**
     * 根据服务名称路由到一个slot槽位, 找到slot槽位所在服务器节点
     * 跟指定服务器节点建立长连接, 然后发送请求到服务器节点进行服务注册
     */
    public void register() throws IOException, InterruptedException {
        Configuration configuration = Configuration.getInstance();
        String serviceInstanceIp = configuration.getServiceInstanceIp();
        int serviceInstancePort = configuration.getServiceInstancePort();
        RegisterServiceRequest registerServiceRequest = new RegisterServiceRequest();
        registerServiceRequest.setInstanceIp(serviceInstanceIp);
        registerServiceRequest.setServicePort(serviceInstancePort);
        registerServiceRequest.setServiceName(configuration.getServiceName());
        registerServiceRequest.buildBuffer();

        LinkedBlockingQueue<Message> queue = getRoutServerSendQueue();
        queue.add(registerServiceRequest);

        String address = "-";
        if (routeServerConnection != null) {
            address = routeServerConnection.getAddress();
        }else{
            throw new NullPointerException("routeServerConnection null");
        }
        log.info("register service {} to server {}.", configuration.getServiceName(), address);

        Response response = getResponseBlocking(registerServiceRequest.getRequestId());

        if (!response.isSuccess()) {
            log.error("register service failed.");
            return;
        }

        log.info("register service success.");
        new HeartbeatThread(responses, routeServerConnection.getConnectionId()).start();
        log.info("service instance heartbeat started ...");
    }

    /**
     * 1. 返回指定服务的所有实例
     * 2. 指定服务若后续实例列表有变更， 则需主动通知客户端
     * @param serviceName
     * @return
     */
    public List<ServiceInstanceInfo> subscribe(String serviceName) throws IOException, InterruptedException {

        if(cacheServiceRegistry.containsKey(serviceName)){
            return cacheServiceRegistry.get(serviceName);
        }

        LinkedBlockingQueue<Message> queue = getRoutServerSendQueue();
        SubscribeRequest subscribeRequest = new SubscribeRequest(serviceName);
        subscribeRequest.buildBuffer();
        queue.add(subscribeRequest);

        Response response = getResponseBlocking(subscribeRequest.getRequestId());

        SubscribeResponse subscribeResponse = SubscribeResponse.parseFrom(response);
        List<ServiceInstanceInfo> addresses = subscribeResponse.getServiceInstanceInfoAddresses();
        cacheServiceRegistry.put(serviceName, addresses);
        log.info("subscribe {} response: {}", serviceName, JSON.toJSONString(addresses));

        return addresses;
    }

    public List<ServiceInstanceInfo> getServiceInstanceAddresses(String serviceName){
        return cacheServiceRegistry.get(serviceName);
    }

    public void onSubscribeService(ServiceChangedRequest notifyRequest) {
        cacheServiceRegistry.put(notifyRequest.getServiceName(), notifyRequest.getServiceInstanceInfoAddresses());
        log.info("notify subscribe service name {}, address: {} ", notifyRequest.getServiceName(),
                JSON.toJSONString(notifyRequest.getServiceInstanceInfoAddresses()));
    }

    private Response getResponseBlocking(long requestId) throws InterruptedException {
        while (responses.get(requestId) == null) {
            Thread.sleep(REQUEST_WAIT_SLEEP_INTERVAL);
        }
        Response response = responses.get(requestId);
        responses.remove(requestId);
        return response;
    }

    private LinkedBlockingQueue<Message> getRoutServerSendQueue() throws IOException, InterruptedException {
        ServerAddress serverAddress = routeServer();
        checkConnection(serverAddress);

        ServerConnection serverConnection = serverConnectionManager.get(serverAddress.toString());
        ServerMessageQueue serverMessageQueue = ServerMessageQueue.getInstance();
        return serverMessageQueue.getMessageQueue(serverConnection.getConnectionId());
    }

    private ServerAddress routeServer() {
        int slot = CommonUtil.routeSlot(Configuration.getInstance().getServiceName());
        String serverNodeId = locateServerNodeBySlot(slot);
        if (serverNodeId == null) {
            log.error("cannot register service to remote server, because the server node id is null.");
            return null;
        }

        Optional<ServerAddress> first = serverAddresses.stream()
                .filter(a -> Objects.equals(a.getNodeId(), serverNodeId)).findFirst();
        if (!first.isPresent()) {
            log.error("cannot register service to remote server, because the server address not found, " +
                    "route node id: {}, server addresses: {}", serverNodeId, JSON.toJSONString(serverAddresses));
            return null;
        }
        return first.get();
    }

    private String locateServerNodeBySlot(int slot) {
        for (String nodeId : slotsMap.keySet()) {
            SlotRange slotRange = slotsMap.get(nodeId);
            if (slot >= slotRange.getStart() && slot <= slotRange.getEnd()) {
                return nodeId;
            }
        }
        return null;
    }

    public void initMetaData(FetchMetaDataResponse response){
        this.slotsMap = response.getSlots();
        this.serverAddresses = response.getServerAddresses();
        // 更新 ServerAddress 实际使用的端口 port
        if(!CollectionUtil.isNullOrEmpty(serverAddresses)){
            for (ServerAddress address: serverAddresses){
                address.setPort(address.getClientTcpPort());
            }
        }
        log.info("init server meta data success.");
        fetchMataDataLatch.countDown();
    }

    private void fetchMetaDataFromServer(String connectionId) {
        FetchMetaDataRequest fetchMetaDataRequest = new FetchMetaDataRequest();
        fetchMetaDataRequest.buildBuffer();
        ServerMessageQueue serverMessageQueue = ServerMessageQueue.getInstance();
        serverMessageQueue.getMessageQueue(connectionId).add(fetchMetaDataRequest);

        try {
            fetchMataDataLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("receive slots from server success: {}", JSON.toJSONString(slotsMap));
    }

    private ServerConnection connectServer(ServerAddress address) throws IOException, InterruptedException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(address.getHost(), address.getPort());
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.socket().setSoLinger(false, -1);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setReuseAddress(true);
        selectionKey = channel.register(selector, SelectionKey.OP_CONNECT);

        channel.connect(inetSocketAddress);

        String addressKey = address.toString();

        log.info("connect server: {}", addressKey);
        ServerConnection serverConnection = null;

        do {
            Thread.sleep(200);
            serverConnection = serverConnectionManager.get(addressKey);
        } while (serverConnection == null);

        return serverConnection;
    }

    private ServerAddress chooseControllerCandidate() {
        Configuration configuration = Configuration.getInstance();
        List<ServerAddress> servers = configuration.getControllerCandidateServers();
        int index = random.nextInt(servers.size());
        ServerAddress serverAddress = servers.get(index);
        log.info("choose server: {}", JSON.toJSONString(serverAddress));
        return serverAddress;
    }

}
