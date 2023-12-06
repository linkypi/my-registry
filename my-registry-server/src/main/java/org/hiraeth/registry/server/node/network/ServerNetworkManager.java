package org.hiraeth.registry.server.node.network;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.common.domain.ServerAddress;
import org.hiraeth.registry.common.util.CollectionUtil;
import org.hiraeth.registry.server.config.Configuration;
import org.hiraeth.registry.server.entity.NodeStatus;
import org.hiraeth.registry.server.entity.RemoteServer;
import org.hiraeth.registry.server.entity.ServerMessage;
import org.hiraeth.registry.server.node.core.NodeInfoManager;
import org.hiraeth.registry.server.node.core.RemoteNodeManager;
import org.hiraeth.registry.server.node.core.ServerConnectionListener;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
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

    private ServerNetworkManager() {
        new RetryConnectServerThread(retryConnectControllers).start();
    }

    public static class Singleton {
        private static final ServerNetworkManager instance = new ServerNetworkManager();
    }
    public static ServerNetworkManager getInstance(){
        return Singleton.instance;
    }

    private static final int CONNECT_TIMEOUT = 5000;

    // 检查跟其他所有节点的连接状态的时间间隔
    private static final long CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL = 5 * 1000L;
    private static final int DEFAULT_RETRIES = 5;

    /**
     * 等待重试发起连接的controller节点集合
     */
    private final CopyOnWriteArrayList<ServerAddress> retryConnectControllers = new CopyOnWriteArrayList<>();
    /**
     * 与其他server节点建立好的连接
     */
    private final Map<String, Socket> remoteServerSockets = new ConcurrentHashMap<>();
    /**
     * 发送队列
     */
    private final Map<String, LinkedBlockingQueue<ServerMessage>> sendQueues = new ConcurrentHashMap<>();

    private final Map<String, IOThreadRunningSignal> ioThreadsRunningSignals = new ConcurrentHashMap<>();

    /**
     * 主动连接排在自身前面的所有机器
     * 如配置有 10,11,12 三台机器，当前机器为12， 则当前机器需要主动连接10及11
     * 这是为了防止机器间的重复连接
     * @return
     */
    public boolean connectOtherControllerServers() {
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
     * 等待 node.id 比当前节点 node.id 大的 controller 节点连接
     */
    public void listenInternalPortAndWaitConnect() {
        new ServerConnectionListener(this).start();
    }

    /**
     * 等待成功连接其他所有节点
     */
    public void waitAllTheOtherControllerConnected() {
        while (NodeInfoManager.getNodeStatus() == NodeStatus.RUNNING) {

            boolean allTheOtherNodesConnected = true;
            List<String> allTheOtherNodeIds = Configuration.getInstance().getAllTheOtherControllerNodeIds();
            if (CollectionUtil.notEmpty(allTheOtherNodeIds)) {
                for (String nodeId : allTheOtherNodeIds) {
                    if (!remoteServerSockets.containsKey(nodeId)) {
                        allTheOtherNodesConnected = false;
                        break;
                    }
                }
                if (allTheOtherNodesConnected) {
                    log.info("established connection with all the other controller nodes.");
                    break;
                }
            }
            List<String> reminds = allTheOtherNodeIds.stream().filter(a -> !remoteServerSockets.containsKey(a)).collect(Collectors.toList());
            log.info("waiting for establishing connection with all the other controller nodes: {}", reminds);
            try {
                Thread.sleep(CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL);
            } catch (InterruptedException e) {
                log.error("thread interrupted when check all the other controller nodes connection status", e);
            }
        }
    }

    /**
     * 发送网络请求
     *
     * @param remoteNodeId
     * @param message
     */
    public void sendRequest(String remoteNodeId, ServerMessage message) {
        try {
            if (sendQueues == null) {
                return;
            }
            LinkedBlockingQueue<ServerMessage> sendQueue = sendQueues.get(remoteNodeId);
            sendQueue.put(message);
        } catch (Exception ex) {
            log.error("put request to send queue failed, remote node id: {}", remoteNodeId, ex);
        }
    }

    public void addRemoteNodeSocket(String remoteNodeId, Socket socket) {
        log.info("add remote node socket, remote node id: {}, address: {}", remoteNodeId, socket.getRemoteSocketAddress());
        this.remoteServerSockets.put(remoteNodeId, socket);
    }

    public boolean connectControllerNode(ServerAddress remoteServerAddress) {

        log.info("connecting other node {}", remoteServerAddress.getNodeId());

        boolean fatalError = false;
        int retries = 0;
        while (NodeInfoManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(remoteServerAddress.getHost(), remoteServerAddress.getInternalPort());

                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setReuseAddress(true);
                socket.connect(endpoint, CONNECT_TIMEOUT);

                // 将当前节点信息发送给其他 server 节点
                if(!sendCurrentNodeInfo(socket)){
                    fatalError = true;
                    break;
                }

                RemoteServer remoteServer = readRemoteNodeInfo(socket);
                if(remoteServer == null){
                    fatalError = true;
                    break;
                }

                addRemoteServerNode(remoteServer);

                // 维护当前连接
                addRemoteNodeSocket(remoteServerAddress.getNodeId(), socket);

                // 为网络连接启动读写IO线程
                startServerIOThreads(remoteServerAddress.getNodeId(), socket);
                return true;
            } catch (IOException ex) {

                log.error("connect controller {} error", remoteServerAddress.getNodeId());
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    log.error("this is {} times retry to connect other controller node {}.",
                            retries, remoteServerAddress.getNodeId());
                }
            }
        }

        // 系统异常崩溃
        if(fatalError) {
            NodeInfoManager.setFatal();
            return false;
        }

        if (!retryConnectControllers.contains(remoteServerAddress)) {
            retryConnectControllers.add(remoteServerAddress);
            log.warn("connect to controller node {} failed, add it into retry connect controller node list.",
                    remoteServerAddress.getNodeId());
        }
        return false;
    }

    public void startServerIOThreads(String remoteNodeId, Socket socket) {

        LinkedBlockingQueue<ServerMessage> sendQueue = new LinkedBlockingQueue<>();
        // 初始化发送请求队列
        sendQueues.put(remoteNodeId, sendQueue);

        IOThreadRunningSignal ioThreadRunningSignal = new IOThreadRunningSignal(true);
        new ServerWriteThread(remoteNodeId, socket, sendQueue, ioThreadRunningSignal).start();
        new ServerReadThread(remoteNodeId, socket, this, ioThreadRunningSignal).start();
        ioThreadsRunningSignals.put(remoteNodeId, ioThreadRunningSignal);
    }

    public void addRemoteServerNode(RemoteServer remoteServer) {
        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        remoteNodeManager.addRemoteServerNode(remoteServer);
        // 更新Configuration实例
        Configuration configuration = Configuration.getInstance();

        if (configuration.getControllerServers().containsKey(remoteServer.getNodeId())) {
            ServerAddress serverAddress = configuration.getControllerServers().get(remoteServer.getNodeId());
            serverAddress.setClientHttpPort(remoteServer.getClientHttpPort());
            serverAddress.setClientTcpPort(remoteServer.getClientTcpPort());
        }
    }

    /**
     * 清理连接资源
     * @param remoteNodeId
     */
    public void clearConnection(String remoteNodeId) {

        RemoteNodeManager remoteNodeManager = RemoteNodeManager.getInstance();
        remoteNodeManager.removeRemoteServerNode(remoteNodeId);
        Socket socket = remoteServerSockets.get(remoteNodeId);
        try {
            socket.close();
        } catch (Exception ex) {
            log.error("close socket occur error when removing remote node", ex);
        }
        remoteServerSockets.remove(remoteNodeId);

        // 必须向队列发送一条消息，才不会使得消费端阻塞在 take() 上
        sendQueues.get(remoteNodeId).add(new ServerMessage(true));
        IOThreadRunningSignal ioThreadRunningSignal = ioThreadsRunningSignals.get(remoteNodeId);
        ioThreadRunningSignal.setIsRunning(false);
        sendQueues.remove(remoteNodeId);
    }

}
