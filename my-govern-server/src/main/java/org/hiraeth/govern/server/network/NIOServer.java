package org.hiraeth.govern.server.network;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.*;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.core.ClientSubscribeQueue;
import org.hiraeth.govern.server.core.RemoteNodeManager;
import org.hiraeth.govern.server.core.SlotManager;
import org.hiraeth.govern.server.core.ClientRequestHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * controller 节点与客户端通信组件
 * @author: lynch
 * @description:
 * @date: 2023/11/30 17:56
 */
@Slf4j
public class NIOServer {

    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private ClientConnectManager clientConnectManager;
    private ClientRequestHandler clientRequestHandler;

    private Map<String, LinkedBlockingDeque<Message>> responseQueues = new ConcurrentHashMap<>();

    public NIOServer(RemoteNodeManager remoteNodeManager, SlotManager slotManager) {
        try {
            this.selector = Selector.open();
            this.clientConnectManager = new ClientConnectManager();
            this.clientRequestHandler = new ClientRequestHandler(
                  remoteNodeManager, slotManager, responseQueues);
        } catch (IOException ex) {
            log.error("controller server selector open failed.", ex);
        }
    }

    public void start() {
        Configuration configuration = Configuration.getInstance();
        ServerAddress currentServerAddress = configuration.getCurrentNodeAddress();
        int tcpPort = currentServerAddress.getClientTcpPort();

        try {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(currentServerAddress.getHost(), tcpPort);

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.socket().bind(inetSocketAddress);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            log.info("nio server port binding {}", tcpPort);

            IOThread ioThread = new IOThread();
            ioThread.setDaemon(true);
            ioThread.start();

        } catch (IOException e) {
            log.error("start nio server occur error: {}", JSON.toJSONString(currentServerAddress), e);
        }
    }

    class IOThread extends Thread {
        @Override
        public void run() {
            ClientConnection connection = null;

            while (!serverSocketChannel.socket().isClosed()) {
                try {
                    int readyChannels = selector.select(1000);
//                    if(readyChannels == 0){
                        Thread.sleep(500);
//                        continue;
//                    }
                    Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    if (selectionKeys == null || selectionKeys.size() == 0) {
                        continue;
                    }

                    Iterator<SelectionKey> iterator = selectionKeys.iterator();
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        connection = (ClientConnection) selectionKey.attachment();

                        if ((selectionKey.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                            accept((ServerSocketChannel) selectionKey.channel());
                            iterator.remove();
                        } else if ((selectionKey.readyOps() & SelectionKey.OP_READ) != 0) {
                            handleRead(connection);
                        } else if ((selectionKey.readyOps() & SelectionKey.OP_WRITE) != 0) {
                            clientRequestHandler.replyResponse(connection);
//                            // 处理客户端请求
//                            replyClient(connection);
//
//                            // 处理客户端订阅信息
//                            clientRequestHandler.notifyClientSubscribe(connection);
                        }
                    }
                }catch (IOException ex) {
                    // 客户端主动断开连接
                    if (connection != null) {
                        try {
                            connection.getSocketChannel().close();
                        } catch (IOException e) {
                            log.error("close client socket channel occur error, connection id: {}", connection.getConnectionId());
                        }
                        clientConnectManager.remove(connection.getConnectionId());
                        log.error("client disconnected, connection id: {}", connection.getConnectionId());
                    } else {
                        log.error("io exception occur.", ex);
                    }

                }
                catch (Exception ex) {
                    log.error("network IO error", ex);
                }
            }
        }

        private void handleRead(ClientConnection connection) throws IOException {
            Message message = connection.doReadIO();
            if (message.getMessageType() == MessageType.REQUEST) {
                Request request = (Request) message;
                handleRequest(request, connection);
            } else if (message.getMessageType() == MessageType.RESPONSE) {
                Response response = (Response) message;
                handleResponse(response);
            } else {
                log.error("unknown message type: {}", message.getMessageType());
            }
        }

        private void handleResponse(Response response) {

        }

        private void handleRequest(Request request, ClientConnection connection) throws IOException {

            Message message = clientRequestHandler.handleRequest(connection, request);
            LinkedBlockingDeque<Message> queue = responseQueues.get(connection.getConnectionId());
            queue.offer(message);
        }

        private void accept(ServerSocketChannel serverChannel) throws IOException {
            // 与客户端完成三次握手建立长连接
            SocketChannel socketChannel = serverChannel.accept();

            if (socketChannel == null) {
                return;
            }

            socketChannel.configureBlocking(false);

            // 将客户端建立好的SocketChannel注册到selector
            SelectionKey clientSelectionKey = socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);

            ClientConnection clientConnection = new ClientConnection(socketChannel, clientSelectionKey);
            clientSelectionKey.attach(clientConnection);
            clientConnectManager.add(clientConnection);

            ClientSubscribeQueue.getInstance().initRequestQueue(clientConnection.getConnectionId());

            log.info("established connection with client: {}", socketChannel.getRemoteAddress());

            responseQueues.put(clientConnection.getConnectionId(), new LinkedBlockingDeque<>());
        }
    }
}
