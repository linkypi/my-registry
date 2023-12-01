package org.hiraeth.govern.server.core;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.ServerAddress;
import org.hiraeth.govern.common.util.CollectionUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.entity.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * 1. 与其他 controller 节点建立连接, 避免出现重复连接
 * 2. 在底层基于队列及线程, 帮助系统发送请求到其他机器节点
 * 3. 在底层基于队列及线程, 帮助系统接收从其他机器节点发送而来的请求
 * @author: lynch
 * @description: 集群内部节点之间进行网络通信的组件
 * @date: 2023/11/27 20:05
 */
@Slf4j
public class ServerNetworkManager extends NetworkManager {

    private static final int CONNECT_TIMEOUT = 5000;
    private static final long RETRY_CONNECT_INTERVAL = 1 * 30 * 1000L;
    // 检查跟其他所有节点的连接状态的时间间隔
    private static final long CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL = 5 * 1000L;
    private static final int DEFAULT_RETRIES = 5;
    /**
     * 等待重试发起连接的controller节点集合
     */
    private CopyOnWriteArrayList<ServerAddress> retryConnectMasterNodes = new CopyOnWriteArrayList<>();
    /**
     * 与其他controller节点建立好的连接
     */
    private Map<String, Socket> remoteMasterNodeSockets = new ConcurrentHashMap<>();
    /**
     * 发送队列
     */
    private Map<String, LinkedBlockingQueue<Message>> sendQueues = new ConcurrentHashMap<>();
    /**
     * 接收队列
     */
    private Map<Integer, LinkedBlockingQueue<MessageBase>> masterReceiveQueues = new ConcurrentHashMap<>();
    // nodeId -> requestType -> response buffer
    private Map<String, Map<Integer, LinkedBlockingQueue<MessageBase>>> slaveReceiveQueues = new ConcurrentHashMap<>();

    /**
     * 远程controller节点管理组件
     */
    private RemoteNodeManager remoteNodeManager;

    public ServerNetworkManager(RemoteNodeManager remoteNodeManager) {
        this.remoteNodeManager = remoteNodeManager;
        new RetryConnectMasterNodeThread().start();
    }

    /**
     * 主动连接比自身node.id 更小的 maser 节点
     * 为防止重复连接，所有controller节点的连接顺序为 1 <- 2 <- 3,
     * 即只有 node.id 较大的节点主动连接 node.id 较小的节点
     *
     * @return
     */
    public boolean connectLowerIdMasterNodes() {
        List<ServerAddress> serverAddresses = Configuration.getInstance().getBeforeControllerAddress();
        if (serverAddresses == null) {
            return true;
        }
        for (ServerAddress item : serverAddresses) {
            connectControllerNode(item);
        }
        return true;
    }

    /**
     * 等待 node.id 比当前节点 node.id 大的master节点连接
     */
    public void waitGreaterIdMasterNodeConnect() {
        new ServerConnectionListener(this).start();
    }

    /**
     * 等待成功连接其他所有节点
     */
    public void waitAllTheOtherNodesConnected() {
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING) {

            boolean allTheOtherNodesConnected = true;
            List<String> allTheOtherNodeIds = Configuration.getInstance().getAllTheOtherNodeIds();
            if (CollectionUtil.notEmpty(allTheOtherNodeIds)) {
                for (String nodeId : allTheOtherNodeIds) {
                    if (!remoteMasterNodeSockets.containsKey(nodeId)) {
                        allTheOtherNodesConnected = false;
                        break;
                    }
                }
                if (allTheOtherNodesConnected) {
                    log.info("established connection with all the other master nodes.");
                    break;
                }
            }
            List<String> reminds = allTheOtherNodeIds.stream().filter(a -> !remoteMasterNodeSockets.containsKey(a)).collect(Collectors.toList());
            log.info("waiting for establishing connection with all the other master nodes: {}", reminds);
            try {
                Thread.sleep(CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL);
            } catch (InterruptedException e) {
                log.error("thread interrupted when check all the other master nodes connection status", e);
            }
        }
    }

    /**
     * 发送网络请求
     * @param remoteNodeId
     * @param message
     * @return
     */
    public boolean sendRequest(String remoteNodeId, Message message) {
        try {
            LinkedBlockingQueue<Message> sendQueue = sendQueues.get(remoteNodeId);
            sendQueue.put(message);
        } catch (Exception ex) {
            log.error("put request to send queue failed, remote node id: {}", remoteNodeId, ex);
            return false;
        }
        return true;
    }

    public MessageBase takeResponseMessage(MessageType messageType){
        try {
            return masterReceiveQueues.get(messageType.getValue()).take();
        }catch (Exception ex){
            log.error("take master message from receive queue failed.", ex);
            return null;
        }
    }

    public int countResponseMessage(MessageType messageType){
        BlockingQueue<MessageBase> queue = masterReceiveQueues.get(messageType.getValue());
        if(queue == null){
            return 0;
        }
        return queue.size();
    }

    public MessageBase takeSlaveMessage(int nodeId, MessageType messageType){
        try {
            return slaveReceiveQueues.get(nodeId).get(messageType.getValue()).take();
        }catch (Exception ex){
            log.error("take slave message from receive queue failed.", ex);
            return null;
        }
    }

    public void addRemoteNodeSocket(String remoteNodeId, Socket socket) {
        log.info("add remote node socket, remote node id: {}, address: {}", remoteNodeId, socket.getRemoteSocketAddress());
        this.remoteMasterNodeSockets.put(remoteNodeId, socket);
    }

    private boolean connectControllerNode(ServerAddress remoteServerAddress) {

        log.info("connecting lower node id master node {}:{}", remoteServerAddress.getHost(), remoteServerAddress.getInternalPort());

        boolean fatalError = false;
        int retries = 0;
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(remoteServerAddress.getHost(), remoteServerAddress.getInternalPort());

                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setReuseAddress(true);
                socket.connect(endpoint, CONNECT_TIMEOUT);

                // 将当前节点信息发送给其他master节点
                if(!sendCurrentNodeInfo(socket)){
                    fatalError = true;
                    break;
                }

                RemoteServer remoteServer = readRemoteNodeInfo(socket);
                if(remoteServer == null){
                    fatalError = true;
                    break;
                }

                addRemoteMasterNode(remoteServer);

                // 维护当前连接
                addRemoteNodeSocket(remoteServerAddress.getNodeId(), socket);

                // 为网络连接启动读写IO线程
                startMasterIOThreads(remoteServerAddress.getNodeId(), socket);
                return true;
            } catch (IOException ex) {

                log.error("connect master node {} error", remoteServerAddress.getNodeId());
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    log.error("this is {} times retry to connect other master node {}.",
                            retries, remoteServerAddress.getNodeId());
                }
            }
        }

        // 系统异常崩溃
        if(fatalError) {
            NodeStatusManager.setFatal();
            return false;
        }

        if (!retryConnectMasterNodes.contains(remoteServerAddress)) {
            retryConnectMasterNodes.add(remoteServerAddress);
            log.warn("connect to master node {} failed, add it into retry connect master node list.",
                    remoteServerAddress.getNodeId());
        }
        return false;
    }

    public void startMasterIOThreads(String remoteNodeId, Socket socket) {

        LinkedBlockingQueue<Message> sendQueue = new LinkedBlockingQueue<>();
        // 初始化发送请求队列
        sendQueues.put(remoteNodeId, sendQueue);

        for (MessageType messageType : MessageType.values()){
            masterReceiveQueues.put(messageType.getValue(), new LinkedBlockingQueue<>());
        }

        new ServerWriteThread(socket, sendQueue).start();
        new ServerReadThread(socket, masterReceiveQueues).start();
    }

    public void startSlaveIOThreads(String remoteNodeId, Socket socket) {

        LinkedBlockingQueue<Message> sendQueue = new LinkedBlockingQueue<>();
        // 初始化发送请求队列
        sendQueues.put(remoteNodeId, sendQueue);

        // 请求类型 -> 响应
        Map<Integer, LinkedBlockingQueue<MessageBase>> slaveReceiveQueue = new ConcurrentHashMap<>();
        for (MessageType messageType : MessageType.values()){
            slaveReceiveQueue.put(messageType.getValue(), new LinkedBlockingQueue<>());
        }
        // 初始化接收队列
        slaveReceiveQueues.put(remoteNodeId, slaveReceiveQueue);

        new ServerWriteThread(socket, sendQueue).start();
        new ServerReadThread(socket, slaveReceiveQueue).start();
    }

    class RetryConnectMasterNodeThread extends Thread {
        @Override
        public void run() {
            while (NodeStatus.RUNNING == NodeStatusManager.getInstance().getNodeStatus()) {
                try {
                    Thread.sleep(RETRY_CONNECT_INTERVAL);
                } catch (InterruptedException e) {
                    log.error("thread interrupted cause of unknown reasons.", e);
                }

                // 重试连接, 连接成功后移除相关节点
                List<ServerAddress> retrySuccessAddresses = new ArrayList<>();
                for (ServerAddress serverAddress : retryConnectMasterNodes) {
                    log.info("scheduled retry connect master node {}.", serverAddress.getNodeId());
                    if (connectControllerNode(serverAddress)) {
                        log.info("scheduled retry connect master node success {}.", serverAddress.getNodeId());
                        retrySuccessAddresses.add(serverAddress);
                    }
                }

                for (ServerAddress address : retrySuccessAddresses) {
                    retryConnectMasterNodes.remove(address);
                }
            }
        }
    }

    public void addRemoteMasterNode(RemoteServer remoteServer) {
        remoteNodeManager.addRemoteServerNode(remoteServer);
    }

}
