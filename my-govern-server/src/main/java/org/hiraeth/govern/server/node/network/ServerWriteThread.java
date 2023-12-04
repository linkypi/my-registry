package org.hiraeth.govern.server.node.network;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.entity.ServerRequestType;
import org.hiraeth.govern.server.node.core.NodeStatusManager;
import org.hiraeth.govern.server.entity.ServerMessage;

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
    private LinkedBlockingQueue<ServerMessage> sendQueue;

    public ServerWriteThread(Socket socket, LinkedBlockingQueue<ServerMessage> sendQueue){
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
                ServerMessage message = sendQueue.take();
                byte[] buffer = message.getBuffer().array();
                outputStream.writeInt(buffer.length);
                outputStream.write(buffer);
                outputStream.flush();

                ServerRequestType requestType = ServerRequestType.of(message.getRequestType());
//                log.info("send message to {}, msg type {}, request type: {}, request id: {}",
//                       message.getToNodeId(), message.getMessageType(), requestType, message.getRequestId());
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
