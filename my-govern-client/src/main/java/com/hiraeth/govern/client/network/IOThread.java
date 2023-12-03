package com.hiraeth.govern.client.network;

import com.hiraeth.govern.client.ServiceInstance;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;
import org.hiraeth.govern.common.domain.request.NotifySubscribeRequest;
import org.hiraeth.govern.common.domain.request.Request;
import org.hiraeth.govern.common.domain.RequestType;
import org.hiraeth.govern.common.domain.response.FetchMetaDataResponse;
import org.hiraeth.govern.common.domain.response.Response;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author: lynch
 * @description:
 * @date: 2023/11/30 22:50
 */
@Slf4j
public class IOThread extends Thread {

    private Selector selector;
    private Map<String, LinkedBlockingQueue<Message>> requestQueue;
    private ServiceInstance serviceInstance;
    private ServerConnectionManager serverConnectionManager;
    private ConcurrentHashMap<Long, Response> responses;
    private BlockingQueue<ServerConnection> reconnectQueue = new LinkedBlockingQueue<>();

    public IOThread(ServiceInstance serviceInstance, ServerConnectionManager serverConnectionManager,
                    ConcurrentHashMap<Long, Response> responses) {
        this.serviceInstance = serviceInstance;
        this.selector = serviceInstance.getSelector();
        this.requestQueue = serviceInstance.getRequestQueue();
        this.serverConnectionManager = serverConnectionManager;
        this.responses = responses;

        new Thread(() -> {
            while (true) {
                try {
                    ServerConnection connection = reconnectQueue.take();
                    reconnectServer(connection);

                } catch (Exception ex) {
                    log.error("reconnect server occur error", ex);
                }
            }
        }).start();
    }

    @Override
    public void run() {
        ServerConnection connection = null;
        SocketChannel socketChannel = null;
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

                    socketChannel = (SocketChannel) selectionKey.channel();
                    connection = (ServerConnection) selectionKey.attachment();
                    if ((selectionKey.readyOps() & SelectionKey.OP_WRITE) != 0) {
                        sendRequest(socketChannel, connection);
                    }
                    if ((selectionKey.readyOps() & SelectionKey.OP_READ) != 0) {
                        handleRead(socketChannel, connection);
                    }
                    if ((selectionKey.readyOps() & SelectionKey.OP_CONNECT) != 0) {
                        handleConnect(socketChannel, selectionKey);
                    }
                }
            }
            catch(ConnectException ex){
                try {
                    log.error("connect exception occur.", ex);
                    if (connection == null) {
                        connection = new ServerConnection(socketChannel);
                    }else{
                        connection.getSocketChannel().close();
                        log.error("server disconnected, connection id: {}", connection.getConnectionId());
                    }
                    reconnectQueue.add(connection);

                }catch (Exception e){
                   log.error("handle connection exception occur error", e);
                }
            }
            catch (IOException ex) {
                // 客户端主动断开连接
                if (connection != null) {
                    reconnectQueue.add(connection);
                } else {
                    log.error("io exception occur.", ex);
                }
            } catch (Exception ex) {
                log.error("network IO occur error", ex);
            }
        }

    }

    private void handleConnect(SocketChannel socketChannel, SelectionKey selectionKey) throws IOException {

        if (socketChannel.finishConnect()) {
            selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);

            ServerConnection connection = new ServerConnection(selectionKey, socketChannel);
            serverConnectionManager.add(connection);

            selectionKey.attach(connection);
            requestQueue.put(connection.getConnectionId(), new LinkedBlockingQueue<>());

            log.info("established connection with server: {}, connection id: {}",
                    socketChannel.getRemoteAddress(), connection.getConnectionId());
        }
    }

    private static final int RETIE_TIMES = 10;
    private void reconnectServer(ServerConnection connection) {

        serviceInstance.getServerConnectionManager().remove(connection);
        int retry = 1;

        while (retry <= RETIE_TIMES) {
            try {
                log.info("reconnection to remote server, retry times {}", retry);
                serviceInstance.reconnect(connection);
                break;
            } catch (Exception ex) {
                log.error("reconnection remote server failed, retry time {}", retry, ex);
                retry++;
            }
        }

    }

    private void handleRead(SocketChannel socketChannel, ServerConnection connection) throws IOException {
        Message message = connection.doReadIO();
        if (message.getMessageType() == MessageType.REQUEST) {
            Request request = (Request) message;
            handlerRequest(request);
        } else if (message.getMessageType() == MessageType.RESPONSE) {
            Response response = (Response) message;
            handlerResponse(response);
        } else {
            log.error("unknown message type: {}", message.getMessageType());
        }
    }

    private void handlerRequest(Request request) {
        if (request.getRequestType() == RequestType.NotifySubscribe) {
            NotifySubscribeRequest notifyRequest = NotifySubscribeRequest.parseFrom(request);
            serviceInstance.onSubscribeService(notifyRequest);
        }
    }

    private void handlerResponse(Response response) {
        if (response.getRequestType() == RequestType.FetchMetaData) {
            FetchMetaDataResponse fetchMetaDataResponse = FetchMetaDataResponse.parseFrom(response);
            serviceInstance.initMetaData(fetchMetaDataResponse);
            return;
        }
        responses.put(response.getRequestId(), response);
    }

    private void sendRequest(SocketChannel socketChannel, ServerConnection connection) throws IOException {

        if (connection == null) {
            return;
        }

        // 从请求队列头部获取一个请求
        LinkedBlockingQueue<Message> queue = requestQueue.get(connection.getConnectionId());
        if (queue == null) {
            return;
        }
        Message message = queue.peek();
        if (message == null || message.getMessageType() != MessageType.REQUEST) {
            return;
        }

        int writeLen = socketChannel.write(message.getBuffer());
//        log.info("socket channel write {} bytes", writeLen);

        // 检查数据是否已经写完, 写完后移除
        if (!message.getBuffer().hasRemaining()) {
            queue.poll();
//            log.info("send request to server, type: {}, id: {}", request.getRequestType(), request.getRequestId());
        }
    }
}