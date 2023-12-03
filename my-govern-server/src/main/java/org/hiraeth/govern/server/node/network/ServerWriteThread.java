package org.hiraeth.govern.server.node.network;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.node.core.NodeStatusManager;
import org.hiraeth.govern.server.entity.ClusterMessage;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 22:07
 */
@Slf4j
public class ServerWriteThread extends Thread{
    /**
     * Server节点之间的网络连接
     */
    private Socket socket;
    private DataOutputStream outputStream;
    /**
     * 发送消息队列
     */
    private LinkedBlockingQueue<ClusterMessage> sendQueue;

    public ServerWriteThread(Socket socket, LinkedBlockingQueue<ClusterMessage> sendQueue){
        this.socket = socket;
        this.sendQueue = sendQueue;
        try {
            this.outputStream = new DataOutputStream(socket.getOutputStream());
        }catch (IOException ex){
            log.error("get data output stream failed..", ex);
        }
    }
    @Override
    public void run() {

        log.info("start write io thread for remote node: {}", socket.getRemoteSocketAddress());

        while (NodeStatusManager.isRunning()) {
            try {
                // 阻塞获取待发送请求
                ClusterMessage clusterMessage = sendQueue.take();
                byte[] buffer = clusterMessage.getBuffer();
                outputStream.writeInt(buffer.length);
                outputStream.write(buffer);
                outputStream.flush();

//                log.info("send message to remote node: {}, message type: {}, message size : {} bytes.",
//                        socket.getRemoteSocketAddress(), message.getMessageType().name(), buffer.length);
            }catch (InterruptedException ex){
                log.error("get message from send queue failed.", ex);
                NodeStatusManager.setFatal();
            }catch (IOException ex){
                log.error("send message to remote node failed.", ex);
                NodeStatusManager.setFatal();
            }
        }

        if(NodeStatusManager.isFatal()){
            log.error("write io thread encounters fatal exception, system is going to shutdown.");
        }
    }
}
