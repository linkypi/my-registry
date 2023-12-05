package org.hiraeth.govern.server.node.core;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.common.domain.ServerAddress;
import org.hiraeth.govern.server.entity.NodeStatus;
import org.hiraeth.govern.server.entity.RemoteServer;
import org.hiraeth.govern.server.node.network.ServerNetworkManager;

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
public class ServerConnectionListener extends Thread{

    /**
     * 网络连接
     */
    private ServerSocket serverSocket;
    private ServerNetworkManager serverNetworkManager;

    private static final int DEFAULT_RETRIES = 3;
    private int retries = 0;

    public ServerConnectionListener(ServerNetworkManager serverNetworkManager) {
        this.serverNetworkManager = serverNetworkManager;
    }

    @Override
    public void run() {
        // 是否遇到了异常崩溃
        boolean fatalError = false;
        while (NodeInfoManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                ServerAddress currentServerAddress = Configuration.getInstance().getCurrentNodeAddress();
                InetSocketAddress endpoint = new InetSocketAddress(currentServerAddress.getHost(), currentServerAddress.getInternalPort());

                serverSocket = new ServerSocket();
                serverSocket.setReuseAddress(true);
                serverSocket.bind(endpoint);

                log.info("server binding {}.", currentServerAddress.getNodeId());

                // 跟发起连接请求的 server 建立网络连接
                while (NodeInfoManager.getNodeStatus() == NodeStatus.RUNNING){
                    Socket socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(0); // 读取数据超时时间为0 ,即无数据时阻塞
                    retries = 0;

                    RemoteServer remoteServer = serverNetworkManager.readRemoteNodeInfo(socket);
                    if(remoteServer == null){
                        fatalError = true;
                        break;
                    }

                    String remoteNodeId = remoteServer.getNodeId();

                    serverNetworkManager.addRemoteServerNode(remoteServer);

                    // 维护建立的连接
                    serverNetworkManager.addRemoteNodeSocket(remoteNodeId, socket);

                    serverNetworkManager.startServerIOThreads(remoteNodeId, socket);

                    // 发送当前节点信息给当前连接节点
                    if(!serverNetworkManager.sendCurrentNodeInfo(socket)){
                        fatalError = true;
                        break;
                    }

                    log.info("established connection with server : {}, remote node id: {}, io threads started.",
                            socket.getRemoteSocketAddress(), remoteNodeId);
                }
            } catch (IOException e) {
                log.error("listening for other server node's connection error.", e);
                retries++;

                if(retries <= DEFAULT_RETRIES){
                    log.error("this is "+ retries +" times retry to listen other server node's connection.");
                }
            }finally {
                try {
                    serverSocket.close();
                } catch (IOException ex) {
                    log.error("closing socket server failed when listening other server node's connection.", ex);
                }
            }
            if(!fatalError){
                break;
            }
        }

        NodeInfoManager.setNodeStatus(NodeStatus.FATAL);
        log.error("failed to listen other server node's connection, although retried {} times, going to shutdown.", DEFAULT_RETRIES);
    }

}
