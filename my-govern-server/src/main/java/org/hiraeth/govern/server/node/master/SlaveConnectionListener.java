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
 * @date: 2023/11/29 10:37
 */
@Slf4j
public class SlaveConnectionListener extends Thread{

    /**
     * 网络连接
     */
    private ServerSocket serverSocket;
    private MasterNetworkManager masterNetworkManager;

    private static final int DEFAULT_RETRIES = 3;
    private int retries = 0;

    public SlaveConnectionListener(MasterNetworkManager masterNetworkManager) {
        this.masterNetworkManager = masterNetworkManager;
    }

    @Override
    public void run() {
        // 是否遇到了异常崩溃
        boolean fatalError = false;
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                MasterAddress currentMasterAddress = Configuration.getInstance().getCurrentNodeAddress();
                InetSocketAddress endpoint = new InetSocketAddress(currentMasterAddress.getHost(), currentMasterAddress.getSlavaPort());

                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(endpoint);

                log.info("slave binding {}:{}.", currentMasterAddress.getHost(), currentMasterAddress.getSlavaPort());

                // 跟发起连接请求的master建立网络连接
                while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING){
                    // id 比自身大的master节点会发送连接请求到此处
                    Socket socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(0); // 读取数据超时时间为0 ,即无数据时阻塞
                    retries = 0;

                    RemoteNode remoteMasterNode = masterNetworkManager.readRemoteNodeInfo(socket);
                    if(remoteMasterNode == null){
                        fatalError = true;
                        break;
                    }

                    int remoteNodeId = remoteMasterNode.getNodeId();

                    // 维护远程slave节点信息
                    masterNetworkManager.addRemoteSlaveNode(remoteMasterNode);

                    // 维护建立的连接
                    masterNetworkManager.addRemoteNodeSocket(remoteNodeId, socket);

                    masterNetworkManager.startSlaveIOThreads(remoteNodeId, socket);


                    log.info("established connection with slave node : {}, remote node id: {}, io threads started.",
                            socket.getRemoteSocketAddress(), remoteNodeId);
                }
            } catch (IOException e) {
                log.error("listening for other slave node's connection error.", e);
                retries++;

                if(retries <= DEFAULT_RETRIES){
                    log.error("this is "+ retries +" times retry to listen other slave node's connection.");
                }
            }finally {
                try {
                    serverSocket.close();
                } catch (IOException ex) {
                    log.error("closing socket server failed when listening other slave node's connection.", ex);
                }
            }
            if(!fatalError){
                break;
            }
        }

        NodeStatusManager.setNodeStatus(NodeStatus.FATAL);
        log.error("failed to listen other slave node's connection, although retried {} times, going to shutdown.", DEFAULT_RETRIES);
    }

}
