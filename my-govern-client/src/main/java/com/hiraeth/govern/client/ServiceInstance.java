package com.hiraeth.govern.client;

import com.alibaba.fastjson.JSON;
import com.hiraeth.govern.client.config.Configuration;
import com.hiraeth.govern.client.network.IOThread;
import com.hiraeth.govern.client.network.ServerConnection;
import com.hiraeth.govern.client.network.ServerConnectionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static org.hiraeth.govern.common.constant.Constant.SLOTS_COUNT;

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

    public ServiceInstance() {
        try {
            selector = Selector.open();
            serverConnectionManager = new ServerConnectionManager();

            new IOThread(this).start();
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
        server = chooseControllerCandidate();
        String connectionId = connectServer(server);
        fetchMetaDataFromServer(connectionId);

        register();
    }

    /**
     * 根据服务名称路由到一个slot槽位, 找到slot槽位所在服务器节点
     * 跟指定服务器节点建立长连接, 然后发送请求到服务器节点进行服务注册
     */
    public void register() {
        int slot = routeSlot();
        String serverNodeId = locateServerNodeBySlot(slot);
        if (serverNodeId == null) {
            log.error("cannot register service to remote server, because the server node id is null.");
            return;
        }

        Optional<ServerAddress> first = serverAddresses.stream().filter(a -> Objects.equals(a.getNodeId(), serverNodeId)).findFirst();
        if (!first.isPresent()) {
            log.error("cannot register service to remote server, because the server address not found, " +
                    "route node id: {}, server addresses: {}", serverNodeId, JSON.toJSONString(serverAddresses));
            return;
        }
        ServerAddress serverAddress = first.get();

        String serviceName = Configuration.getInstance().getServiceName();
        log.info("register service {} to server node {}.", serviceName, serverAddress.getClientNodeId());

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

    private int routeSlot(){
        String serviceName = Configuration.getInstance().getServiceName();
        // 为防止负数需要 & 操作
        int hashCode = serviceName.hashCode() & Integer.MAX_VALUE;
        return hashCode % SLOTS_COUNT;
    }

    public void initMetaData(FetchMetaDataResponse response){
        this.slotsMap = response.getSlots();
        this.serverAddresses = response.getServerAddresses();
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

    private String connectServer(ServerAddress address) throws IOException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(address.getHost(), address.getInternalPort());
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.socket().setSoLinger(false, -1);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setReuseAddress(true);
        selectionKey = channel.register(selector, SelectionKey.OP_CONNECT);

        channel.connect(inetSocketAddress);

        log.info("try to connect server: {}", JSON.toJSONString(address));

        return waitConnect();
    }

    private String waitConnect() throws IOException {
        boolean finishedConnect = false;
        while (!finishedConnect) {
            selector.select();
            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            if (selectionKeys.isEmpty()) {
                continue;
            }
            for (SelectionKey selectionKey : selectionKeys) {
                if ((selectionKey.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                    SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                    if (socketChannel.finishConnect()) {
                        finishedConnect = true;
                        selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);

                        ServerConnection serverConnection = new ServerConnection(selectionKey, socketChannel);
                        serverConnectionManager.add(serverConnection);

                        selectionKey.attach(serverConnection);
                        requestQueue.put(serverConnection.getConnectionId(), new LinkedBlockingQueue<>());

                        log.info("established connection with server, connection id: {}", serverConnection.getConnectionId());
                        return serverConnection.getConnectionId();
                    }
                }
            }
        }
        return null;
    }

    private static final Random random = new Random();

    private ServerAddress chooseControllerCandidate() {
        Configuration configuration = Configuration.getInstance();
        List<ServerAddress> servers = configuration.getControllerCandidateServers();
        int index = random.nextInt(servers.size());
        ServerAddress serverAddress = servers.get(index);
        log.info("choose server: {}", JSON.toJSONString(serverAddress));
        return serverAddress;
    }

}
