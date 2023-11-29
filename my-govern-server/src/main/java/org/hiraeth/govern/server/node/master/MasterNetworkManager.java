package org.hiraeth.govern.server.node.master;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.util.CollectionUtil;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.master.entity.NodeAddress;
import org.hiraeth.govern.server.node.master.entity.NodeStatus;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.master.entity.RemoteMasterNode;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

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
    private static final long RETRY_CONNECT_INTERVAL = 1 * 30 * 1000L;
    // 检查跟其他所有节点的连接状态的时间间隔
    private static final long CHECK_ALL_OTHER_NODES_CONNECT_INTERVAL = 5 * 1000L;
    private static final int DEFAULT_RETRIES = 5;
    /**
     * 等待重试发起连接的Master节点集合
     */
    private CopyOnWriteArrayList<NodeAddress> retryConnectMasterNodes = new CopyOnWriteArrayList<>();
    /**
     * 与其他master节点建立好的连接
     */
    private Map<Integer, Socket> remoteMasterNodeSockets = new ConcurrentHashMap<>();
    /**
     * 发送队列
     */
    private Map<Integer, LinkedBlockingQueue<ByteBuffer>> sendQueues = new ConcurrentHashMap<>();
    /**
     * 接收队列
     */
    private BlockingQueue<ByteBuffer> receiveQueue = new LinkedBlockingQueue<>();

    /**
     * 远程master节点管理组件
     */
    private RemoteMasterNodeManager remoteMasterNodeManager;

    public MasterNetworkManager(RemoteMasterNodeManager remoteMasterNodeManager) {
        this.remoteMasterNodeManager = remoteMasterNodeManager;
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
        new MasterConnectionListener(this).start();
    }

    /**
     * 等待成功连接其他所有节点
     */
    public void waitAllTheOtherNodesConnected() {
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING) {

            boolean allTheOtherNodesConnected = true;
            List<Integer> allTheOtherNodeIds = Configuration.getInstance().getAllTheOtherNodeIds();
            if (CollectionUtil.notEmpty(allTheOtherNodeIds)) {
                for (Integer nodeId : allTheOtherNodeIds) {
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
            List<Integer> reminds = allTheOtherNodeIds.stream().filter(a -> !remoteMasterNodeSockets.containsKey(a)).collect(Collectors.toList());
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
    public boolean sendRequest(Integer remoteNodeId, ByteBuffer message) {
        try {
            LinkedBlockingQueue<ByteBuffer> sendQueue = sendQueues.get(remoteNodeId);
            sendQueue.put(message);
        } catch (Exception ex) {
            log.error("put request to send queue failed, remote node id: {}", remoteNodeId, ex);
            return false;
        }
        return true;
    }

    public ByteBuffer takeMessage(){
        try {
            return receiveQueue.take();
        }catch (Exception ex){
            log.error("take message from receive queue failed.", ex);
            return null;
        }
    }

    public void addRemoteNodeSocket(int remoteNodeId, Socket socket) {
        log.info("add remote master node socket, remote node id: {}, address: {}", remoteNodeId, socket.getRemoteSocketAddress());
        this.remoteMasterNodeSockets.put(remoteNodeId, socket);
    }

    private boolean connectMasterNode(NodeAddress remoteNodeAddress) {

        log.info("connecting lower node id master node {}:{}", remoteNodeAddress.getHost(), remoteNodeAddress.getMasterPort());

        boolean fatalError = false;
        int retries = 0;
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(remoteNodeAddress.getHost(), remoteNodeAddress.getMasterPort());

                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setReuseAddress(true);
                socket.connect(endpoint, CONNECT_TIMEOUT);

                // 将当前节点信息发送给其他master节点
                if(!sendCurrentNodeInfo(socket)){
                    fatalError = true;
                    break;
                }

                RemoteMasterNode remoteMasterNode = readRemoteNodeInfo(socket);
                if(remoteMasterNode == null){
                    fatalError = true;
                    break;
                }

                addRemoteMasterNode(remoteMasterNode);

                // 维护当前连接
                addRemoteNodeSocket(remoteNodeAddress.getNodeId(), socket);

                // 为网络连接启动读写IO线程
                startMasterIOThreads(remoteNodeAddress.getNodeId(), socket);
                return true;
            } catch (IOException ex) {

                log.error("connect master node({}:{}) error", remoteNodeAddress.getHost(), remoteNodeAddress.getMasterPort());
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    log.error("this is {} times retry to connect other master node({}:{}).",
                            retries, remoteNodeAddress.getHost(), remoteNodeAddress.getMasterPort());
                }
            }
        }

        // 系统异常崩溃
        if(fatalError) {
            NodeStatusManager.setFatal();
            return false;
        }

        if (!retryConnectMasterNodes.contains(remoteNodeAddress)) {
            retryConnectMasterNodes.add(remoteNodeAddress);
            log.warn("connect to master node({}:{}) failed, add it into retry connect master node list.",
                    remoteNodeAddress.getHost(), remoteNodeAddress.getMasterPort());
        }
        return false;
    }

    /**
     * 向master机器发送自身 nodeId
     * @param socket
     * @return
     */
    public boolean sendCurrentNodeInfo(Socket socket) {
        Configuration configuration = Configuration.getInstance();
        int nodeId = configuration.getNodeId();
        boolean isControllerCandidate = configuration.isControllerCandidate();

        RemoteMasterNode remoteMasterNode = new RemoteMasterNode(nodeId, isControllerCandidate);
        ByteBuffer buffer = remoteMasterNode.toBuffer();

        DataOutputStream outputStream = null;
        try {
            outputStream = new DataOutputStream(socket.getOutputStream());
            outputStream.writeInt(buffer.array().length);
            outputStream.write(buffer.array());
            outputStream.flush();
        }catch (IOException ex){
            log.error("send self node info to other master node failed.", ex);
            try {
                socket.close();
            }catch (IOException e){
                log.error("close socket failed when sending self node info to other master node.", e);
            }
            return false;
        }catch (Exception ex){
            log.error("send self node info to other master node failed.", ex);
        }
        return true;
    }

    public RemoteMasterNode readRemoteNodeInfo(Socket socket) {
        try {
            DataInputStream inputStream = new DataInputStream(socket.getInputStream());

            int length = inputStream.readInt();
            byte[] bytes = new byte[length];
            inputStream.readFully(bytes);
            return RemoteMasterNode.parseFrom(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            log.error("reading remote node id failed", e);

            try {
                socket.close();
            } catch (IOException ex) {
                log.error("closing socket failed when reading remote node id failed......", ex);
            }
        }
        return null;
    }

    public void startMasterIOThreads(int remoteNodeId, Socket socket) {

        LinkedBlockingQueue<ByteBuffer> queue = new LinkedBlockingQueue<>();
        // 初始化发送请求队列
        sendQueues.put(remoteNodeId, queue);

        new MasterNetworkWriteThread(socket, queue).start();
        new MasterNetworkReadThread(socket, receiveQueue).start();

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
                            nodeAddress.getHost(), nodeAddress.getMasterPort());
                    if (connectMasterNode(nodeAddress)) {
                        log.info("scheduled retry connect master node success {}:{}.",
                                nodeAddress.getHost(), nodeAddress.getMasterPort());
                        retrySuccessAddresses.add(nodeAddress);
                    }
                }

                for (NodeAddress address : retrySuccessAddresses) {
                    retryConnectMasterNodes.remove(address);
                }
            }
        }
    }

    public void addRemoteMasterNode(RemoteMasterNode remoteMasterNode) {
        remoteMasterNodeManager.addRemoteMasterNode(remoteMasterNode);
    }
}
