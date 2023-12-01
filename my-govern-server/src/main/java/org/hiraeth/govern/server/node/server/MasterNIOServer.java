package org.hiraeth.govern.server.node.server;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.hiraeth.govern.common.domain.BaseRequest;
import org.hiraeth.govern.common.domain.BaseResponse;
import org.hiraeth.govern.common.domain.Response;
import org.hiraeth.govern.server.config.Configuration;
import org.hiraeth.govern.server.node.entity.NodeAddress;
import org.hiraeth.govern.server.node.master.SlotManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * master 节点与客户端通信组件
 * @author: lynch
 * @description:
 * @date: 2023/11/30 17:56
 */
@Slf4j
public class MasterNIOServer {

    private Selector selector;
    private ServerSocketChannel serverSocketChannel;
    private ClientConnectManager clientConnectManager;
    private ClientRequestHandler clientRequestHandler;

    private Map<String, LinkedBlockingDeque<Response>> responseQueues = new ConcurrentHashMap<>();

    public MasterNIOServer(SlotManager slotManager) {
        try {
            this.clientConnectManager = new ClientConnectManager();
            this.clientRequestHandler = new ClientRequestHandler(slotManager);
            this.selector = Selector.open();
        } catch (IOException ex) {
            log.error("master server selector open failed.", ex);
        }
    }

    public void start() {
        Configuration configuration = Configuration.getInstance();
        NodeAddress currentNodeAddress = configuration.getCurrentNodeAddress();
        int externalPort = currentNodeAddress.getExternalPort();

        try {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(currentNodeAddress.getHost(), externalPort);

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().setReuseAddress(true);
            serverSocketChannel.socket().bind(inetSocketAddress);
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            log.info("nio server binding to port {}", externalPort);

            IOThread ioThread = new IOThread();
            ioThread.setDaemon(true);
            ioThread.start();

        } catch (IOException e) {
            log.error("start nio server occur error: {}", JSON.toJSONString(currentNodeAddress), e);
        }
    }

    class IOThread extends Thread {
        @Override
        public void run() {

            while (!serverSocketChannel.socket().isClosed()) {
                try {

                    int readyChannels = selector.select(1000);
                    if(readyChannels == 0){
                        Thread.sleep(500);
                        continue;
                    }
                    Set<SelectionKey> selectionKeys = selector.selectedKeys();
                    if (selectionKeys == null || selectionKeys.size() == 0) {
                        continue;
                    }

                    Iterator<SelectionKey> iterator = selectionKeys.iterator();
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        iterator.remove();

                        if ((selectionKey.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                            accept(selectionKey);
                        } else if ((selectionKey.readyOps() & SelectionKey.OP_READ) != 0) {
                            if (selectionKey.isReadable()) {
                                handleRequest(selectionKey);
                            }
                        } else if ((selectionKey.readyOps() & SelectionKey.OP_WRITE) != 0) {
                            if (selectionKey.isReadable()) {
                                replyClient(selectionKey);
                            }
                        }
                    }
                } catch (Exception ex) {
                    log.error("network IO error", ex);
                }
            }
        }

        private void handleRequest( SelectionKey selectionKey) {
            ClientConnection connection = (ClientConnection) selectionKey.attachment();
            BaseRequest request = connection.doReadIO();
            if (request == null) {
                return;
            }
            Response response = clientRequestHandler.handle(request);
            LinkedBlockingDeque<Response> queue = responseQueues.get(connection.getConnectionId());
            queue.offer(response);
        }

        private void replyClient(SelectionKey selectionKey) {
            try {
                ClientConnection connection = (ClientConnection) selectionKey.attachment();
                LinkedBlockingDeque<Response> queue = responseQueues.get(connection.getConnectionId());
                if (queue.isEmpty()) {
                    return;
                }
                Response response = queue.peek();
                if (response == null) {
                    return;
                }

                SocketChannel socketChannel = connection.getSocketChannel();
                socketChannel.write(response.getBuffer());

                if (!response.getBuffer().hasRemaining()) {
                    queue.poll();
                }
            } catch (Exception ex) {
                log.error("reply client occur error", ex);
            }
        }

        private void accept(SelectionKey selectionKey) throws IOException {
            // 与客户端完成三次握手建立长连接
            ServerSocketChannel serverChannel = (ServerSocketChannel) selectionKey.channel();
            SocketChannel socketChannel = serverChannel.accept();

            if (socketChannel == null) {
                return;
            }

            socketChannel.configureBlocking(false);

            // 将客户端建立好的SocketChannel注册到selector
            SelectionKey clientSelectionKey = socketChannel.register(selector, SelectionKey.OP_READ);

            ClientConnection clientConnection = new ClientConnection(socketChannel, clientSelectionKey);
            clientSelectionKey.attach(clientConnection);
            clientConnectManager.add(clientConnection);
            log.info("established connection with client: {}", socketChannel.getRemoteAddress());

            responseQueues.put(clientConnection.getConnectionId(), new LinkedBlockingDeque<>());
        }
    }
}
