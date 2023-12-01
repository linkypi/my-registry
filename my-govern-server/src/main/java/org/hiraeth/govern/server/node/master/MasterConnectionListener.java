package org.hiraeth.govern.server.node.master;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.common.domain.MasterAddress;
import org.hiraeth.govern.server.node.entity.NodeStatus;
import org.hiraeth.govern.server.node.entity.RemoteNode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/28 22:27
 */
@Slf4j
public class MasterConnectionListener extends Thread{

    /**
     * 网络连接
     */
    private ServerSocket serverSocket;
    private MasterNetworkManager masterNetworkManager;

    private static final int DEFAULT_RETRIES = 3;
    private int retries = 0;

    public MasterConnectionListener(MasterNetworkManager masterNetworkManager) {
        this.masterNetworkManager = masterNetworkManager;
    }

    @Override
    public void run() {
        // 是否遇到了异常崩溃
        boolean fatalError = false;
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                MasterAddress currentMasterAddress = Configuration.getInstance().getCurrentNodeAddress();
                InetSocketAddress endpoint = new InetSocketAddress(currentMasterAddress.getHost(), currentMasterAddress.getMasterPort());

                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(endpoint);

                log.info("master binding {}:{}.", currentMasterAddress.getHost(), currentMasterAddress.getMasterPort());

                // 跟发起连接请求的master建立网络连接
                while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING){
                    // id 比自身大的master节点会发送连接请求到此处
                    Socket socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(0); // 读取数据超时时间为0 ,即无数据时阻塞
                    retries = 0;

                    RemoteNode remoteNode = masterNetworkManager.readRemoteNodeInfo(socket);
                    if(remoteNode == null){
                        fatalError = true;
                        break;
                    }

                    int remoteNodeId = remoteNode.getNodeId();

                    masterNetworkManager.addRemoteMasterNode(remoteNode);

                    // 维护建立的连接
                    masterNetworkManager.addRemoteNodeSocket(remoteNodeId, socket);

                    masterNetworkManager.startMasterIOThreads(remoteNodeId, socket);

                    // 发送当前节点信息给当前连接节点
                    if(!masterNetworkManager.sendCurrentNodeInfo(socket)){
                        fatalError = true;
                        break;
                    }

                    log.info("established connection with master node : {}, remote node id: {}, io threads started.",
                            socket.getRemoteSocketAddress(), remoteNodeId);
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

        NodeStatusManager.setNodeStatus(NodeStatus.FATAL);
        log.error("failed to listen other master node's connection, although retried {} times, going to shutdown.", DEFAULT_RETRIES);
    }

}
