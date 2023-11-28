package org.hiraeth.govern.server.node.master;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeAddress;
import org.hiraeth.govern.server.node.NodeStatus;
import org.hiraeth.govern.server.node.NodeStatusManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 1. 与其他master节点建立连接, 避免出现重复连接
 * 2. 在底层基于队列及线程, 帮助系统发送请求到其他机器节点
 * 3. 在底层基于队列及线程, 帮助系统接收从其他机器节点发送而来的请求
 * @author: lynch
 * @description: 集群内部节点之间进行网络通信的组件
 * @date: 2023/11/27 20:05
 */
@Slf4j
public class MasterNetworkManager {

    private static final int CONNECT_TIMEOUT = 5000;
    private static final long RETRY_CONNECT_INTERVAL = 1 * 60 * 1000L;
    // 检查跟其他所有节点的连接状态的时间间隔
    private static final long CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL = 5 * 1000L;
    private static final int DEFAULT_RETRIES = 3;
    /**
     * 等待重试发起连接的Master节点集合
     */
    private CopyOnWriteArrayList<NodeAddress> retryConnectMasterNodes = new CopyOnWriteArrayList<>();
    /**
     * 与其他master节点建立好的连接
     */
    private Map<Integer, Socket> remoteMasterNodeSockets = new ConcurrentSkipListMap<>();

    public MasterNetworkManager() {
        new RetryConnectMasterNodeThread().start();
    }

    /**
     * 主动连接比自身node.id 更小的 maser 节点
     * 为防止重复连接，所有master节点的连接顺序为 1 <- 2 <- 3,
     * 即只有 node.id 较大的节点主动连接 node.id 较小的节点
     *
     * @return
     */
    public boolean connectLowerIdMasterNodes() {
        List<NodeAddress> nodeAddresses = Configuration.getInstance().getLowerIdMasterAddress();
        if (nodeAddresses == null) {
            return true;
        }
        for (NodeAddress item : nodeAddresses) {
            connectMasterNode(item);
        }
        return true;
    }

    /**
     * 等待 node.id 比当前节点 node.id 大的master节点连接
     */
    public void waitGreaterIdMasterNodeConnect() {
        new MasterConnectionListener().start();
    }

    /**
     * 等待成功连接其他所有节点
     */
    public void waitAllTheOtherNodesConnected() {

        List<Integer> allTheOtherNodeIds = Configuration.getInstance().getAllTheOtherNodeIds();
        NodeStatusManager statusManager = NodeStatusManager.getInstance();
        while (statusManager.getNodeStatus() == NodeStatus.RUNNING){

            boolean allTheOtherNodesConnected = true;
            for (Integer nodeId: allTheOtherNodeIds){
                if (!remoteMasterNodeSockets.containsKey(nodeId)) {
                    allTheOtherNodesConnected = false;
                    break;
                }
            }

            if(allTheOtherNodesConnected){
                log.info("established connection with all the other master nodes.");
                break;
            }
            log.info("waiting for establishing connection with all the other master nodes.");
            try {
                Thread.sleep(CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL);
            } catch (InterruptedException e) {
                log.error("thread interrupted when check all the other master nodes connection status", e);
            }
        }



    }

    private boolean addRemoteNodeSocket(Socket socket) {
        InetSocketAddress remoteAddress = (InetSocketAddress) socket.getRemoteSocketAddress();
        String hostName = remoteAddress.getHostName();
        Integer remoteNodeId = Configuration.getInstance().getNodeIdByHostName(hostName);
        if(remoteNodeId == null){
            // 此时直接认定整个集群启动失败
            log.error("established connection is not in the right remote address: {}, " +
                    "because cannot found the node id by hostname: {}", remoteAddress, hostName);
            try {
                socket.close();
            }catch (IOException ex){
                log.error("closing connection in unknown remote address {} failed, " +
                        "because cannot found the node id by hostname: {}", remoteAddress, hostName);
            }
            return false;
        }

        this.remoteMasterNodeSockets.put(remoteNodeId, socket);
        return true;
    }

    private boolean connectMasterNode(NodeAddress nodeAddress) {

        log.info("connecting lower node id master node {}:{}", nodeAddress.getHostname(), nodeAddress.getMasterMasterPort());

        int retries = 0;
        NodeStatusManager statusManager = NodeStatusManager.getInstance();
        while (statusManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(nodeAddress.getHostname(), nodeAddress.getMasterMasterPort());

                Socket socket = new Socket();
                socket.setReuseAddress(true);
                socket.connect(endpoint, CONNECT_TIMEOUT);

                // 为网络连接启动读写IO线程
                startIOThreads(socket);

                // 维护当前连接
                addRemoteNodeSocket(socket);

                return true;
            } catch (IOException ex) {

                log.error("connect master node({}:{}) error", nodeAddress.getHostname(), nodeAddress.getMasterMasterPort());
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    log.error("this is {} times retry to connect other master node({}:{}).",
                            retries, nodeAddress.getHostname(), nodeAddress.getMasterMasterPort());
                }
            }
            return false;
        }

        if (!retryConnectMasterNodes.contains(nodeAddress)) {
            retryConnectMasterNodes.add(nodeAddress);
            log.warn("connect to master node({}:{}) failed, add it into retry connect master node list.",
                    nodeAddress.getHostname(), nodeAddress.getMasterMasterPort());
        }
        return false;
    }

    private void startIOThreads(Socket socket) {
        new MasterNetworkWriteThread(socket).start();
        new MasterNetworkReadThread(socket).start();
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
                List<NodeAddress> retrySuccessAddresses = new ArrayList<>();
                for (NodeAddress nodeAddress : retryConnectMasterNodes) {
                    log.info("scheduled retry connect master node {}:{}.",
                            nodeAddress.getHostname(), nodeAddress.getMasterMasterPort());
                    if (connectMasterNode(nodeAddress)) {
                        log.info("scheduled retry connect master node success {}:{}.",
                                nodeAddress.getHostname(), nodeAddress.getMasterMasterPort());
                        retrySuccessAddresses.add(nodeAddress);
                    }
                }

                for (NodeAddress address : retrySuccessAddresses) {
                    retryConnectMasterNodes.remove(address);
                }
            }
        }
    }

    class MasterConnectionListener extends Thread{

        /**
         * 网络连接
         */
        private ServerSocket serverSocket;
        private volatile boolean running = true;

        private static final int DEFAULT_RETRIES = 3;
        private int retries = 0;

        @Override
        public void run() {
            // 是否遇到了异常崩溃
            boolean fatalError = false;
            NodeStatusManager statusManager = NodeStatusManager.getInstance();
            while (statusManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
                try {
                    int port = getCurrentMasterNetworkPort();
                    InetSocketAddress endpoint = new InetSocketAddress(port);

                    serverSocket = new ServerSocket();
                    serverSocket.setReuseAddress(true);
                    serverSocket.bind(endpoint);

                    log.info("master binding port: {}.", port);

                    // 跟发起连接请求的master建立网络连接
                    while (statusManager.getNodeStatus() == NodeStatus.RUNNING){
                        // id 比自身大的master节点会发送连接请求到此处
                        Socket socket = serverSocket.accept();
                        socket.setTcpNoDelay(true);
                        socket.setSoTimeout(0); // 读取数据超时时间为0 ,即无数据时阻塞
                        retries = 0;

                        startIOThreads(socket);

                        // 维护建立的连接
                        if(!addRemoteNodeSocket(socket)){
                            // 远程master节点添加失败，系统异常崩溃
                            fatalError = true;
                            break;
                        }

                        log.info("established connection with master node : {}, io threads started.", socket.getRemoteSocketAddress());
                    }
                } catch (IOException e) {
                    log.error("listening for other master node's connection error.", e);
                    retries++;

                    if(retries <= DEFAULT_RETRIES){
                        log.error("this is "+ retries +" times retry to listen other master node's connection.");
                    }
                }finally {
                    try {
                        serverSocket.close();
                    } catch (IOException ex) {
                        log.error("closing socket server failed when listening other master node's connection.", ex);
                    }
                }
                if(!fatalError){
                    break;
                }
            }

            NodeStatusManager.getInstance().setNodeStatus(NodeStatus.FATAL);
            log.error("failed to listen other master node's connection, although retried {} times, going to shutdown system.", DEFAULT_RETRIES);
        }

        private int getCurrentMasterNetworkPort() {
            NodeAddress currentNodeAddress = Configuration.getInstance().getCurrentNodeAddress();
            return currentNodeAddress.getMasterMasterPort();
        }

        public void shutdown(){
            this.running = false;
        }
    }
}
