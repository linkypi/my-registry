package org.hiraeth.registry.server.node.network;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.registry.server.node.core.NodeInfoManager;
import org.hiraeth.registry.server.entity.ServerMessage;

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
    private String remoteNodeId;
    /**
     * 发送消息队列
     */
    private LinkedBlockingQueue<ServerMessage> sendQueue;
    private IOThreadRunningSignal ioThreadRunningSignal;

    public ServerWriteThread(String remoteNodeId, Socket socket, LinkedBlockingQueue<ServerMessage> sendQueue,
                             IOThreadRunningSignal ioThreadRunningSignal) {

        try {
            this.socket = socket;
            this.sendQueue = sendQueue;
            this.remoteNodeId = remoteNodeId;
            this.ioThreadRunningSignal = ioThreadRunningSignal;
            this.outputStream = new DataOutputStream(socket.getOutputStream());
            this.setName(remoteNodeId + "-" + ServerWriteThread.class.getSimpleName());
        } catch (IOException ex) {
            log.error("get data output stream failed..", ex);
        }
    }
    @Override
    public void run() {

        log.info("start write io thread for remote node: {}", socket.getRemoteSocketAddress());

        while (NodeInfoManager.isRunning() && ioThreadRunningSignal.getIsRunning()) {
            try {
                // 阻塞获取待发送请求
                ServerMessage message = sendQueue.take();
                if(message.isTerminated()){
                    break;
                }
                byte[] buffer = message.getBuffer().array();
                outputStream.writeInt(buffer.length);
                outputStream.write(buffer);
                outputStream.flush();

//                ServerRequestType requestType = ServerRequestType.of(message.getRequestType());
//                log.info("send message to {}, msg type {}, request type: {}, request id: {}",
//                       message.getToNodeId(), message.getMessageType(), requestType, message.getRequestId());
            }catch (InterruptedException ex){
                log.error("get message from send queue failed.", ex);
                NodeInfoManager.setFatal();
            }catch (IOException ex){
                log.error("send message to remote node failed.", ex);
                NodeInfoManager.setFatal();
            }
        }

        String address = socket.getRemoteSocketAddress().toString();
        if(NodeInfoManager.isFatal()){
            log.error("write io thread encounters fatal exception with remote node: {}, system is going to shutdown.", remoteNodeId);
        }else{
            log.info("write io thread exit of remote node: {}.", remoteNodeId);
        }
    }
}
