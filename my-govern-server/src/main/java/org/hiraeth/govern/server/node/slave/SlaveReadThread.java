package org.hiraeth.govern.server.node.slave;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.server.node.NodeStatusManager;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/29 12:01
 */
@Slf4j
public class SlaveReadThread extends Thread{
    /**
     * master节点之间的网络连接
     */
    private Socket socket;
    private DataInputStream inputStream;
    private BlockingQueue<ByteBuffer> receiveQueue;

    public SlaveReadThread(Socket socket, BlockingQueue<ByteBuffer> receiveQueue){
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
        while (NodeStatusManager.isRunning()){

            try {
                // 从IO流读取一条消息
                int length = inputStream.readInt();
                byte[] bytes = new byte[length];
                inputStream.readFully(bytes,0,length);

                // 将字节数组封装成byteBuffer
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                receiveQueue.put(buffer);

                log.info("get message from remote node: {}, message size is {} bytes",
                        socket.getRemoteSocketAddress(), buffer.capacity());
            }catch (IOException ex){
                log.error("read message from remote node failed.", ex);
                NodeStatusManager.setFatal();
            } catch (InterruptedException ex) {
                log.error("put message into receive queue failed.", ex);
                NodeStatusManager.setFatal();
            }
        }

        if(NodeStatusManager.isFatal()){
            log.error("read io thread encounters fatal exception, system is going to shutdown.");
        }
    }
}
