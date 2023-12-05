package org.hiraeth.govern.server.node.network;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.entity.ServerMessage;
import org.hiraeth.govern.server.entity.ServerRequestType;
import org.hiraeth.govern.server.ha.HighAvailabilityManager;
import org.hiraeth.govern.server.node.core.NodeInfoManager;
import org.hiraeth.govern.server.node.core.ServerMessageQueue;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 22:08
 */
@Slf4j
public class ServerReadThread extends Thread{
    /**
     * Server 节点之间的网络连接
     */
    private Socket socket;
    private DataInputStream inputStream;
    private String remoteNodeId;
    private ServerNetworkManager serverNetworkManager;
    private IOThreadRunningSignal ioThreadRunningSignal;

    public ServerReadThread(String remoteNodeId, Socket socket, ServerNetworkManager serverNetworkManager,
                            IOThreadRunningSignal ioThreadRunningSignal){

        try {
            this.remoteNodeId = remoteNodeId;
            this.socket = socket;
            this.ioThreadRunningSignal = ioThreadRunningSignal;
            this.serverNetworkManager = serverNetworkManager;
            this.inputStream = new DataInputStream(socket.getInputStream());
            this.setName(remoteNodeId + "-" + ServerReadThread.class.getSimpleName());
            socket.setSoTimeout(0);
        }catch (IOException ex){
            log.error("get input stream from socket failed.", ex);
        }
    }

    @Override
    public void run() {

        log.info("start read io thread for remote node: {}", remoteNodeId);
        while (NodeInfoManager.isRunning() && ioThreadRunningSignal.getIsRunning()) {

            try {
                // 从IO流读取一条消息
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.readFully(bytes, 0, length);

                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                ServerMessage message = ServerMessage.parseFromBuffer(buffer);

                ServerMessageQueue messageQueue = ServerMessageQueue.getInstance();
                messageQueue.addMessage(message);
                ServerRequestType requestType = ServerRequestType.of(message.getRequestType());
                log.info("receive message from {}, msg type {}, request type: {}, request id: {}",
                       message.getFromNodeId(), message.getMessageType(), requestType, message.getRequestId());
            } catch (EOFException | SocketException ex) {
                log.error("remote server disconnected {}", remoteNodeId, ex);

                serverNetworkManager.clearConnection(remoteNodeId);

                HighAvailabilityManager highAvailabilityManager = HighAvailabilityManager.getInstance();
                highAvailabilityManager.handleDisconnectedException(remoteNodeId);

            }catch (Exception ex) {
                log.error("read message from remote node failed.", ex);
                NodeInfoManager.setFatal();
            }
        }

        if(NodeInfoManager.isFatal()){
            log.error("remote node {} read io thread encounters fatal exception, system is going to shutdown.", remoteNodeId);
        }else{
            log.info("read io thread exit for remote node {}.", remoteNodeId);
        }
    }
}
