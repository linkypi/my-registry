package org.hiraeth.govern.server.node.slave;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.NetworkManager;
import org.hiraeth.govern.server.node.NodeStatusManager;
import org.hiraeth.govern.server.node.entity.NodeStatus;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/29 11:41
 */
@Slf4j
public class SlaveNetworkManager extends NetworkManager{

    private static final int CONNECT_TIMEOUT = 5000;
    private static final int DEFAULT_RETRIES = 5;

    private Socket masterNodeSocket;

    private LinkedBlockingQueue<ByteBuffer> sendQueue = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<ByteBuffer> receiveQueue = new LinkedBlockingQueue<>();

    public boolean connectToMasterNode(){

        Configuration configuration = Configuration.getInstance();
        String masterServerAddress = configuration.getMasterServerAddress();
        int masterServerPort = configuration.getMasterServerPort();

        log.info("connecting to master node {}:{}", masterServerAddress, masterServerPort);

        boolean fatalError = false;
        int retries = 0;
        while (NodeStatusManager.getNodeStatus() == NodeStatus.RUNNING && retries <= DEFAULT_RETRIES) {
            try {
                InetSocketAddress endpoint = new InetSocketAddress(masterServerAddress, masterServerPort);

                Socket socket = new Socket();
                socket.setTcpNoDelay(true);
                socket.setReuseAddress(true);
                socket.connect(endpoint, CONNECT_TIMEOUT);

                // 将当前节点信息发送给其他master节点
                if(!sendCurrentNodeInfo(socket)){
                    fatalError = true;
                    break;
                }

                // 为网络连接启动读写IO线程
                startIOThreads(socket);

                this.masterNodeSocket = socket;

                return true;
            } catch (IOException ex) {

                log.error("connect slave node {}:{} failed", masterServerAddress,masterServerPort);
                retries++;
                if (retries <= DEFAULT_RETRIES) {
                    log.error("this is {} times retry to connect master node {}:{}.",
                            retries, masterServerAddress,masterServerPort);
                }
            }
        }

        if(retries > DEFAULT_RETRIES){
            fatalError = true;
            log.error("max retries {} times to connect master node failed, system is going to shutdown.", DEFAULT_RETRIES);
        }

        // 系统异常崩溃
        if(fatalError) {
            NodeStatusManager.setFatal();
            return false;
        }

        return false;
    }

    public void startIOThreads(Socket socket) {
        new SlaveWriteThread(socket, sendQueue).start();
        new SlaveReadThread(socket, receiveQueue).start();
    }
}
