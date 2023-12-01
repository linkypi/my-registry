package com.hiraeth.govern.client;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.BaseResponse;
import org.hiraeth.govern.common.domain.Request;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:50
 */
@Slf4j
public class IOThread extends Thread{

    private Selector selector;
    private Map<String, LinkedBlockingQueue<Request>> requestQueue;

    public IOThread(Selector selector, Map<String, LinkedBlockingQueue<Request>> requestQueue){
        this.selector = selector;
        this.requestQueue = requestQueue;
    }
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(500);
                int count = selector.select(300);
                if(count == 0){
                    continue;
                }
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                if (selectionKeys == null || selectionKeys.size() == 0) {
                    continue;
                }

                for (SelectionKey selectionKey : selectionKeys) {
                    SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                    ServerConnection connection = (ServerConnection)selectionKey.attachment();
                    if ((selectionKey.readyOps() & SelectionKey.OP_WRITE) != 0) {
                        if (selectionKey.isWritable()) {
                            sendRequest(socketChannel, connection);
                        }
                    }
                    if ((selectionKey.readyOps() & SelectionKey.OP_READ) != 0) {
                        if (selectionKey.isReadable()) {
                            handleResponse(socketChannel);
                        }
                    }
                }

            } catch (Exception ex) {
                log.error("network IO occur error", ex);
            }
        }

    }

    private void handleResponse(SocketChannel socketChannel) {

    }

    private void sendRequest(SocketChannel socketChannel,ServerConnection connection) throws IOException {
        // 从请求队列头部获取一个请求
        LinkedBlockingQueue<Request> queue = requestQueue.get(connection.getConnectionId());
        Request request = queue.peek();
        if(request == null){
            return;
        }

        int writeLen = socketChannel.write(request.getBuffer());
        log.info("socket channel write {} bytes", writeLen);

        // 检查数据是否已经写完, 写完后移除
        if(!request.getBuffer().hasRemaining()){
            queue.poll();
            log.info("send request to server, type: {}, id: {}", request.getRequestType(), request.getRequestId());
        }
    }
}
