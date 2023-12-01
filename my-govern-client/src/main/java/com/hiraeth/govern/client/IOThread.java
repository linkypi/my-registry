package com.hiraeth.govern.client;

import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:50
 */
@Slf4j
public class IOThread extends Thread {

    private Selector selector;
    private Map<String, LinkedBlockingQueue<Request>> requestQueue;
    private ClientServer clientServer;

    public IOThread(ClientServer clientServer) {
        this.clientServer = clientServer;
        this.selector = clientServer.getSelector();
        this.requestQueue = clientServer.getRequestQueue();
    }

    @Override
    public void run() {
        ServerConnection connection = null;
        while (true) {
            try {
                int readyChannels = selector.select(300);
//                if(readyChannels == 0){
                Thread.sleep(500);
//                    continue;
//                }
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                if (selectionKeys == null || selectionKeys.size() == 0) {
                    continue;
                }
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    iterator.remove();

                    SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                    connection = (ServerConnection) selectionKey.attachment();
                    if ((selectionKey.readyOps() & SelectionKey.OP_WRITE) != 0) {
                        sendRequest(socketChannel, connection);
                    }
                    if ((selectionKey.readyOps() & SelectionKey.OP_READ) != 0) {
                        handleResponse(socketChannel, connection);
                    }
                }
            } catch (IOException ex) {
                // 客户端主动断开连接
                if (connection != null) {
                    reconnectionServer(connection);
                } else {
                    log.error("io exception occur.", ex);
                }

            } catch (Exception ex) {
                log.error("network IO occur error", ex);
            }
        }

    }

    private static final int RETIE_TIMES = 10;
    private void reconnectionServer(ServerConnection connection) {

        clientServer.getServerConnectionManager().remove(connection);
        int retry = 1;

        while (retry <= RETIE_TIMES) {
            try {
                log.info("reconnection to remote server, retry times {}", retry);
                clientServer.init();
                break;
            } catch (Exception ex) {
                log.error("reconnection remote server failed, retry time {}", retry, ex);
                retry++;
            }
        }

    }

    private void handleResponse(SocketChannel socketChannel, ServerConnection connection) throws IOException {
        BaseResponse response = connection.doReadIO();
        if (response.getRequestType() == RequestType.FetchMetaData) {
            FetchMetaDataResponse fetchMetaDataResponse = FetchMetaDataResponse.parseFrom(response);
            clientServer.initMetaData(fetchMetaDataResponse);
        }
    }

    private void sendRequest(SocketChannel socketChannel, ServerConnection connection) throws IOException {

        if (connection == null) {
            return;
        }

        // 从请求队列头部获取一个请求
        LinkedBlockingQueue<Request> queue = requestQueue.get(connection.getConnectionId());
        if (queue == null) {
            return;
        }
        Request request = queue.peek();
        if (request == null) {
            return;
        }

        int writeLen = socketChannel.write(request.getBuffer());
        log.info("socket channel write {} bytes", writeLen);

        // 检查数据是否已经写完, 写完后移除
        if (!request.getBuffer().hasRemaining()) {
            queue.poll();
            log.info("send request to server, type: {}, id: {}", request.getRequestType(), request.getRequestId());
        }
    }
}