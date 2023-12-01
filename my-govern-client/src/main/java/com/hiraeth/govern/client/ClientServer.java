package com.hiraeth.govern.client;

import com.alibaba.fastjson.JSON;
import com.hiraeth.govern.client.config.Configuration;
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
public class ClientServer {

    // io 多路复用组件
    private Selector selector;
    // 客户端与服务端的selectionKey
    private SelectionKey selectionKey;

    private Map<String, LinkedBlockingQueue<Request>> requestQueue = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, BaseResponse> responses = new ConcurrentHashMap<>();

    private ServerConnectionManager serverConnectionManager;
    private Map<Integer, SlotRang> slotsMap;
    private List<MasterAddress> masterAddresses;
    private CountDownLatch fetchMataDataLatch = new CountDownLatch(1);

    public ClientServer() {
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
        MasterAddress masterAddress = chooseMasterServer();
        String connectionId = connectServer(masterAddress);
        fetchMetaDataFromServer(connectionId);

        register();
    }

    /**
     * 根据服务名称路由到一个slot槽位, 找到slot槽位所在master节点
     * 跟指定master节点建立长连接, 然后发送请求到master节点进行服务注册
     */
    public void register() {
        int slot = routeSlot();
        Integer masterNodeId = locateMasterNodeBySlot(slot);
        if (masterNodeId == null) {
            log.error("cannot register service to remote master, because the master node id is null.");
            return;
        }

        Optional<MasterAddress> first = masterAddresses.stream().filter(a -> a.getNodeId() == masterNodeId).findFirst();
        if (!first.isPresent()) {
            log.error("cannot register service to remote master, because the master address not found, " +
                    "route node id: {}, master addresses: {}", masterNodeId, JSON.toJSONString(masterAddresses));
            return;
        }
        MasterAddress masterAddress = first.get();

        String serviceName = Configuration.getInstance().getServiceName();
        log.info("register service {} to master node {}.", serviceName, masterAddress.getNodeId());

    }

    public void startHeartbeatSchedule() {

    }

    private Integer locateMasterNodeBySlot(int slot) {
        for (Integer nodeId : slotsMap.keySet()) {
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
        this.masterAddresses = response.getMasterAddresses();
        log.info("init master meta data success.");
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
        log.info("receive slots from master server success: {}", JSON.toJSONString(slotsMap));
    }

    private String connectServer(MasterAddress address) throws IOException {
        InetSocketAddress inetSocketAddress = new InetSocketAddress(address.getHost(), address.getExternalPort());
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.socket().setSoLinger(false, -1);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setReuseAddress(true);
        selectionKey = channel.register(selector, SelectionKey.OP_CONNECT);

        channel.connect(inetSocketAddress);

        log.info("try to connect server: {}", address);

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

    private MasterAddress chooseMasterServer() {
        Configuration configuration = Configuration.getInstance();
        List<MasterAddress> masterServers = configuration.getMasterServers();
        int index = random.nextInt(masterServers.size());
        MasterAddress masterAddress = masterServers.get(index);
        log.info("choose master server: {}", masterAddress);
        return masterAddress;
    }

}
