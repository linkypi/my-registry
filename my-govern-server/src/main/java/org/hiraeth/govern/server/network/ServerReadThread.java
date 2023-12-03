package org.hiraeth.govern.server.network;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.core.NodeStatusManager;
import org.hiraeth.govern.server.entity.ClusterBaseMessage;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

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
    private Map<Integer,LinkedBlockingQueue<ClusterBaseMessage>> receiveQueue;

    public ServerReadThread(Socket socket, Map<Integer, LinkedBlockingQueue<ClusterBaseMessage>> receiveQueue){
        this.socket = socket;
        this.receiveQueue = receiveQueue;
        try {
            this.inputStream = new DataInputStream(socket.getInputStream());
            socket.setSoTimeout(0);
        }catch (IOException ex){
            log.error("get input stream from socket failed.", ex);
        }
    }
    @Override
    public void run() {

        log.info("start read io thread for remote node: {}", socket.getRemoteSocketAddress());
        while (NodeStatusManager.isRunning()) {

            try {
                // 从IO流读取一条消息
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.readFully(bytes, 0, length);

                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                ClusterBaseMessage messageBase = ClusterBaseMessage.parseFromBuffer(buffer);
                int msgType = messageBase.getClusterMessageType().getValue();

                receiveQueue.get(msgType).add(messageBase);

//                MessageType msgTypeEnum = messageBase.getMessageType();
//                log.info("get message from remote node: {}, message type: {}, message size: {} bytes",
//                        socket.getRemoteSocketAddress(), msgTypeEnum.name(), buffer.capacity());
            } catch (IOException ex) {
                log.error("read message from remote node failed.", ex);
                NodeStatusManager.setFatal();
            }
        }

//        if (NodeStatusManager.isFatal()) {
//            log.error("read io thread encounters fatal exception, system is going to shutdown.");
//        }
    }
}
