package org.hiraeth.govern.server.node.master;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.node.NodeStatusManager;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/27 22:07
 */
@Slf4j
public class MasterNetworkWriteThread extends Thread{
    /**
     * master节点之间的网络连接
     */
    private Socket socket;
    private DataOutputStream outputStream;
    /**
     * 发送消息队列
     */
    private LinkedBlockingQueue<ByteBuffer> sendQueue = new LinkedBlockingQueue<>();

    public MasterNetworkWriteThread(Socket socket, LinkedBlockingQueue<ByteBuffer> sendQueue){
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
                ByteBuffer buffer = sendQueue.take();

                outputStream.writeInt(buffer.capacity());
                outputStream.write(buffer.array());
                outputStream.flush();

                log.info("sending message to remote node: {}, message size is {} bytes.",
                        socket.getRemoteSocketAddress(), buffer.capacity());
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
