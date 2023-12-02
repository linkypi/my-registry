package com.hiraeth.govern.client;

import com.alibaba.fastjson.JSON;
import com.hiraeth.govern.client.config.Configuration;
import com.hiraeth.govern.client.network.HeartbeatThread;
import com.hiraeth.govern.client.network.IOThread;
import com.hiraeth.govern.client.network.ServerConnection;
import com.hiraeth.govern.client.network.ServerConnectionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;
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

    private Map<String, LinkedBlockingQueue<Request>> requestQueue = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, BaseResponse> responses = new ConcurrentHashMap<>();

    private ServerConnectionManager serverConnectionManager;
    private Map<String, SlotRang> slotsMap;
    private List<ServerAddress> serverAddresses;
    // 当前连接的服务器
    private ServerAddress server;
    private CountDownLatch fetchMataDataLatch = new CountDownLatch(1);

    private ServerConnection controllerConnection;
    private ServerConnection routeServerConnection;

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
    public void init() throws IOException {
        connectController();
        registerCurrentInstance();
    }

    private void registerCurrentInstance() throws IOException {
        ServerAddress serverAddress = routeServer();
        // 判断是否已存在相同连接
        if (controllerConnection != null && controllerConnection.getAddress().equals(serverAddress.getNodeId())) {
            routeServerConnection = controllerConnection;
        } else {
            routeServerConnection = connectServer(server);
        }
        routeServerConnection = connectServer(serverAddress);
        register();
    }

    private void connectController() throws IOException {
        server = chooseControllerCandidate();

        // 判断是否已存在相同连接
        if (routeServerConnection != null && routeServerConnection.getAddress().equals(server.getNodeId())) {
            controllerConnection = routeServerConnection;
        } else {
            controllerConnection = connectServer(server);
        }

        fetchMetaDataFromServer(controllerConnection.getConnectionId());
    }

    public void reconnect(ServerConnection connection) throws IOException {

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
    public void register() {
        Configuration configuration = Configuration.getInstance();
        String serviceInstanceIp = configuration.getServiceInstanceIp();
        int serviceInstancePort = configuration.getServiceInstancePort();
        RegisterServiceRequest registerServiceRequest = new RegisterServiceRequest();
        registerServiceRequest.setInstanceIp(serviceInstanceIp);
        registerServiceRequest.setServicePort(serviceInstancePort);
        registerServiceRequest.setServiceName(configuration.getServiceName());
        Request request = registerServiceRequest.toRequest();
        requestQueue.get(routeServerConnection.getConnectionId()).add(request);
        log.info("register service {} to server {}.", configuration.getServiceName(), routeServerConnection.getAddress());

        new HeartbeatThread(requestQueue, responses, routeServerConnection.getConnectionId()).start();
        log.info("service instance heartbeat started ...");
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

    public void startHeartbeatSchedule() {

    }

    private String locateServerNodeBySlot(int slot) {
        for (String nodeId : slotsMap.keySet()) {
            SlotRang slotRang = slotsMap.get(nodeId);
            if (slot >= slotRang.getStart() && slot <= slotRang.getEnd()) {
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
        requestQueue.get(connectionId).add(fetchMetaDataRequest.toRequest());

        try {
            fetchMataDataLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        log.info("receive slots from server success: {}", JSON.toJSONString(slotsMap));
    }

    private ServerConnection connectServer(ServerAddress address) throws IOException {
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
